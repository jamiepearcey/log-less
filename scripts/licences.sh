#!/usr/bin/env bash
# Dependency licence check.
#
# log-less itself is BSL, which is only defensible if everything under it is
# permissive: a copyleft dependency would impose terms on users we have not told
# them about, and finding that out at the point of a customer's legal review is
# too late. Runs from `cargo metadata`, so it needs no extra tooling and works
# offline.
set -uo pipefail
cd "$(dirname "$0")/.."
cargo metadata --format-version 1 --all-features | python3 -c '
import json, sys

# Permissive only. Deliberately no "or later" wildcards and no GPL/AGPL/SSPL of
# any flavour, including LGPL: dynamic-linking exceptions do not survive a
# statically linked Rust binary.
ALLOWED = {
    "MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception", "BSD-2-Clause",
    "BSD-3-Clause", "ISC", "Zlib", "Unicode-3.0", "Unicode-DFS-2016", "CC0-1.0",
    "MIT-0", "BSL-1.0", "Unlicense", "0BSD",
    # The licence of Mozilla'"'"'s CA bundle: permissive, and not copyleft.
    "CDLA-Permissive-2.0",
}
# Crates in this workspace are ours; their licence is the project licence.
OURS = {"logless-core", "logless-cli"}

metadata = json.load(sys.stdin)
problems, unlicensed = [], []
for package in metadata["packages"]:
    name = package["name"]
    if name in OURS:
        continue
    licence = package.get("license")
    if not licence:
        # license-file rather than an SPDX expression: cannot be checked
        # mechanically, so it is reported for a human rather than passed.
        unlicensed.append((name, package.get("license_file")))
        continue
    # An SPDX expression: every OR branch permissive is enough, every AND
    # branch must be.
    # Cargo'"'"'s legacy separator is a slash, not " OR " — treating it as one
    # expression rejects half of crates.io for no reason.
    expression = licence.replace("(", "").replace(")", "").replace("/", " OR ")
    branches = [b.strip() for b in expression.split(" OR ")]
    if not any(all(part.strip() in ALLOWED for part in b.split(" AND ")) for b in branches):
        problems.append((name, licence))

for name, licence in sorted(problems):
    print(f"DISALLOWED  {name}: {licence}")
for name, path in sorted(unlicensed):
    print(f"NO SPDX     {name}: license-file={path}")

total = len(metadata["packages"]) - len(OURS)
if problems:
    print(f"\nFAIL: {len(problems)} of {total} dependencies are not permissively licensed")
    sys.exit(1)
if unlicensed:
    print(f"\nFAIL: {len(unlicensed)} of {total} dependencies declare no SPDX licence")
    sys.exit(1)
print(f"all {total} dependencies are permissively licensed")
'
