#!/usr/bin/env bash
# Disk-full and io-pressure injection.
#
# `chaos.sh` kills the process; this fills the filesystem underneath it. They
# fail differently: a SIGKILL stops everything at once, while ENOSPC hits one
# write at a time, so the agent keeps running with some subsystems failing and
# others fine. That is the case where a component quietly swallows an error and
# reports success, and no unit test reaches it because the filesystem is real.
#
# Uses a fixed-size disk image, so nothing can escape onto the real filesystem.
# macOS: hdiutil. Linux: a loopback ext4 image (needs sudo) or a tmpfs mount.
#
# Usage: scripts/diskfull.sh [image-MB]
set -uo pipefail

SIZE_MB="${1:-24}"
BIN="${LOGLESS_BIN:-./target/release/logless}"
[[ -x "$BIN" ]] || { echo "build first: cargo build --release" >&2; exit 1; }

MOUNT=""; DEVICE=""; IMAGE=""
cleanup() {
  [[ -n "$DEVICE" ]] && hdiutil detach "$DEVICE" -quiet 2>/dev/null
  [[ -n "$MOUNT" && "$(uname)" == "Linux" ]] && sudo umount "$MOUNT" 2>/dev/null
  [[ -n "$IMAGE" ]] && rm -f "$IMAGE"
  [[ -n "$MOUNT" && -d "$MOUNT" ]] && rmdir "$MOUNT" 2>/dev/null
  return 0
}
trap cleanup EXIT

case "$(uname)" in
  Darwin)
    # No mktemp for the image: it creates the file, and hdiutil then refuses to
    # write over it. Let hdiutil own the path and append its own extension.
    base="/tmp/loglessdisk.$$"
    IMAGE="${base}.dmg"
    MOUNT="/tmp/loglessdisk-mnt.$$"
    mkdir -p "$MOUNT"
    if ! hdiutil create -size "${SIZE_MB}m" -fs HFS+ -volname loglessdisk -o "$base" -quiet; then
      echo "could not create the disk image" >&2; exit 1
    fi
    if ! hdiutil attach "$IMAGE" -nobrowse -quiet -mountpoint "$MOUNT"; then
      echo "could not attach the disk image" >&2; exit 1
    fi
    DEVICE=$(hdiutil info | awk -v m="$MOUNT" '$0 ~ m {print $1; exit}')
    ;;
  Linux)
    MOUNT="$(mktemp -d)"
    if ! sudo mount -t tmpfs -o "size=${SIZE_MB}m" tmpfs "$MOUNT" 2>/dev/null; then
      echo "SKIP: need sudo to mount a size-limited tmpfs" >&2
      exit 0
    fi
    ;;
  *) echo "SKIP: unsupported platform" >&2; exit 0 ;;
esac

# Only the agent's *data directory* lives on the small filesystem. The
# harness's own files stay outside: a test that cannot write its own log when
# the subject runs out of disk reports nothing, and "everything is full" is a
# different, less interesting failure than "the agent's storage is full".
WORK="$(mktemp -d)"
trap 'cleanup; rm -rf "$WORK"' EXIT

echo "small filesystem at $MOUNT (${SIZE_MB} MiB); harness files in $WORK"
"$BIN" init --data-dir "$MOUNT/data" > "$WORK/config.toml"
# Budget larger than the filesystem, deliberately: the agent's own pressure
# relief must not be what saves it. The point is what happens when the *disk*
# says no, not when our accounting does.
sed -i.bak "s/^disk_budget_bytes = .*/disk_budget_bytes = $((SIZE_MB * 4 * 1024 * 1024))/" \
  "$WORK/config.toml"

echo "filling the filesystem while the agent writes..."
# Sized so the agent's own WAL and store fit on the filesystem once the
# ballast is gone. Any larger and the disk stays full after freeing, which
# tests the operator's problem rather than the agent's.
awk -v n="$((SIZE_MB * 4000))" 'BEGIN { for (i = 0; i < n; i++)
  printf "%s\tpayment gateway timeout for order %d trace_id=%032x\n",
    (i % 5 == 0 ? "ERROR" : "DEBUG"), i, i }' > "$WORK/input.txt"

# Ballast that grows until ENOSPC, so the agent meets a genuinely full disk
# mid-run rather than a comfortable one.
# Two pressures at once. The ballast leaves the filesystem genuinely full
# mid-run; the churn keeps the device busy so writes are slow as well as
# failing, which is the combination that exposes a code path assuming an fsync
# either succeeds quickly or not at all.
#
# Real io throttling wants cgroup v2 `io.max` on Linux, which needs privileges
# a CI runner does not reliably have. Contention is the portable approximation
# and it is honest about being one.
( sleep 0.5; dd if=/dev/zero of="$MOUNT/ballast" bs=1m 2>/dev/null ) &
ballast=$!
(
  for _ in $(seq 1 40); do
    dd if=/dev/zero of="$MOUNT/churn" bs=64k count=16 conv=fsync 2>/dev/null
    rm -f "$MOUNT/churn"
  done
) &
churn=$!

"$BIN" run --config "$WORK/config.toml" < "$WORK/input.txt" > "$WORK/run.out" 2>&1
status=$?
wait "$ballast" 2>/dev/null
wait "$churn" 2>/dev/null
rm -f "$MOUNT/ballast" "$MOUNT/churn"

# Whether ENOSPC lands on a critical write depends on timing, so the exit
# status is reported rather than asserted. What is asserted below holds either
# way: nothing invented, nothing corrupted, and the store still readable.
echo "--- agent exit: $status"
grep -E "^received=|ACCOUNTING" "$WORK/run.out" || true
echo "--- errors reported while the disk was full"
grep -ciE "no space|enospc" "$WORK/run.out" || true

# What must hold: the agent may lose data it never claimed to have taken, but
# it must not claim success for records it dropped, and it must not corrupt
# what it did commit.
if grep -q "ACCOUNTING VIOLATION" "$WORK/run.out"; then
  echo "FAIL: accounting violation under ENOSPC" >&2
  exit 1
fi
# An operator frees space, and the agent must then recover what survived —
# which is the actual recovery story, not "recover while still full".
echo "--- freeing space and recovering"
"$BIN" recover --config "$WORK/config.toml" --repair >> "$WORK/run.out" 2>&1
if ! "$BIN" merge --config "$WORK/config.toml" >> "$WORK/run.out" 2>&1; then
  echo "note: merge did not complete"
fi
if ! "$BIN" catalog --config "$WORK/config.toml" --rebuild > "$WORK/rebuild.out" 2>&1; then
  echo "FAIL: the catalog could not be rebuilt from what survived" >&2
  cat "$WORK/rebuild.out" >&2
  exit 1
fi
head -3 "$WORK/rebuild.out"
# A generous tolerance: ENOSPC legitimately loses whatever could not be
# written. What is being asserted is that nothing is *invented* and the store
# is still readable — `verify` fails on a negative or an over-count.
"$BIN" verify --config "$WORK/config.toml" --tolerance 100000000 || {
  echo "FAIL: verify rejected the store after a full disk" >&2; exit 1; }
echo "disk-full harness passed"
