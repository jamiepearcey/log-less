# Sync soak: flapping peer, restarts, exact accounting

**What this is:** about three minutes of injected faults on one machine (Apple M4 Max, rustc 1.91.0, debug build),
run on 2026-10-11 from branch `hard/sync`. **What it is not:** the 72-hour soak, a crash soak with `kill -9`, or a
test across a real network. Plane items 47/48/49 stay open.

## Command

```sh
CARGO_TARGET_DIR=$HOME/tmp/logless-hard-sync-target \
LOGLESS_SOAK_SECS=180 cargo test -p logless-core --lib -- sync::soak --include-ignored --nocapture
```

`crates/logless-core/src/sync_soak.rs`. Two tests ran side by side: the one-round test that runs with every
`cargo test -p logless-core --lib -- sync:: peer::`, and the ignored `soak_for_minutes`, which repeats the round for
`LOGLESS_SOAK_SECS` seconds.

## What one round does

A real sender (`sync::run_tuned`, backoff unit 10 ms, spool cap 256 KiB) sends to a real receiver
(`peer::Receiver::start_in`, dedupe log on disk, random bearer token) through a TCP proxy that cycles: 200, 503, 200,
429, 200, 401, 200, connection dropped mid-body, 200, forwarded with the reply dropped (the receiver stores the batch,
the sender never hears), 200, 422 (dropped for good). Every tenth record is a loop for the receiver (its origin is the
receiver's id).

1. 40 batches of 25 through the fault cycle; the sender is stopped and restarted after the 21st.
2. Peer down (all 503): batches of 50 until the spool sheds, then drain through the fault cycle.
3. One batch is forwarded and its reply dropped, the receiver is shut down and restarted on the same data dir, and the
   resend must come back as duplicates from the dedupe log.
4. The last 100 stored records are offered again (provenance stripped, as a replay from the edge's own store would be).

Then a two-node ring: A sends to B, B's receiver feeds B's sender, which sends back to A.

## How the ledger is checked (all asserted, the test fails otherwise)

* **Copies:** every record handed to the sender has one fate:
  `offered = accepted + duplicates + loops + rejected + shed`, where accepted, duplicates and loops are what the
  receiver told the sender (summed over sender restarts), rejected is counted by the proxy from the 422 bodies, and
  shed is the sender's `dropped_records` (tap or spool full). Duplicates are extra copies: re-offers and resends of
  batches whose reply was dropped.
* **Sender against proxy:** records in relayed replies = sender accepted + duplicates + loops.
* **Receiver against proxy:** records the receiver parsed = relayed + reply dropped = accepted + duplicates + loops.
* **Distinct ids:** no event id stored twice; every non-loop copy forwarded was stored once or dropped as a duplicate;
  no loop stored; ids neither stored, refused as loop nor rejected = shed, so **nothing is missing** beyond what was
  shed and counted.
* Spool on disk never above its cap (+4 KiB for the cursor files), nothing pending at the end, every fault kind
  injected at least once, the dedupe log recognised resends after a receiver restart.
* Ring: B accepts every record once, A refuses every one as a loop and accepts none, and no request is made in the
  1.5 s after the lap.

Shed is large because phase 2 fills the spool on purpose and the tap keeps queuing batches while the sender waits out
503s; every shed record is counted.

## Output

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.65s
     Running unittests src/lib.rs (/Users/jamiepearcey/tmp/logless-hard-sync-target/debug/deps/logless_core-7e3f7a2f2f3e516d)

running 2 tests
rounds 1

=== log-less sync soak: exact accounting (12.0 s of injected faults) ===
sender restarts 1, receiver restarts 1, spool fills 1
proxy requests 57:
  200 pass               29
  503                    6
  429                    5
  401                    4
  drop mid-body          4
  forward, drop reply    5
  422                    4
--- copies (every record handed to the sender, re-offers included) ---
offered copies                          5330
  of which re-offers of stored records  100
sender: accepted (unique, heard)        653
sender: duplicates                      239
sender: loops                           88
proxy:  rejected (422, dropped for good) 125
sender: shed (tap or spool full)        4225
offered = accepted + duplicates + loops + rejected + shed:  5330 = 653 + 239 + 88 + 125 + 4225 = 5330
--- receiver (both incarnations) ---
records parsed 1135 = accepted 792 + duplicates 239 + loops 104
proxy forwarded and answered: relayed 980 + reply dropped 155 = 1135
duplicates recognised by a restarted receiver: 27
dedupe batches not saved: 0
--- distinct event ids ---
offered ids 5230 (loops injected 523)
stored once 792 (stored rows 792), refused as loop 88, rejected 125
not stored, not refused, not rejected: 4225  (must equal shed 4225)
rejected batches seen by the sender: 4
--- spool ---
max spool bytes on disk 258298 (cap 262144); at the end 60739 on disk (the current segment, already delivered), 0 pending
--- two-node ring A -> B -> A ---
offered at A 500; B accepted 500; A refused as loop 500; A accepted 0
requests after the lap and 1.5 s later: A 10 -> 10, B 10 -> 10
A backlog 0 bytes, B backlog 0 bytes
test sync::soak::a_flapping_peer_loses_nothing_and_every_record_is_accounted_for ... ok
test sync::soak::soak_for_minutes has been running for over 60 seconds
rounds 17

=== log-less sync soak: exact accounting (191.0 s of injected faults) ===
sender restarts 17, receiver restarts 17, spool fills 17
proxy requests 1147:
  200 pass               574
  503                    107
  429                    90
  401                    90
  drop mid-body          90
  forward, drop reply    107
  422                    89
--- copies (every record handed to the sender, re-offers included) ---
offered copies                          57060
  of which re-offers of stored records  1700
sender: accepted (unique, heard)        12291
sender: duplicates                      4613
sender: loops                           1681
proxy:  rejected (422, dropped for good) 2700
sender: shed (tap or spool full)        35775
offered = accepted + duplicates + loops + rejected + shed:  57060 = 12291 + 4613 + 1681 + 2700 + 35775 = 57060
--- receiver (both incarnations) ---
records parsed 21820 = accepted 15204 + duplicates 4613 + loops 2003
proxy forwarded and answered: relayed 18585 + reply dropped 3235 = 21820
duplicates recognised by a restarted receiver: 459
dedupe batches not saved: 0
--- distinct event ids ---
offered ids 55360 (loops injected 5536)
stored once 15204 (stored rows 15204), refused as loop 1681, rejected 2700
not stored, not refused, not rejected: 35775  (must equal shed 35775)
rejected batches seen by the sender: 89
--- spool ---
max spool bytes on disk 258814 (cap 262144); at the end 60840 on disk (the current segment, already delivered), 0 pending
--- two-node ring A -> B -> A ---
offered at A 500; B accepted 500; A refused as loop 500; A accepted 0
requests after the lap and 1.5 s later: A 10 -> 10, B 10 -> 10
A backlog 0 bytes, B backlog 0 bytes
test sync::soak::soak_for_minutes ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 599 filtered out; finished in 194.20s

LOGLESS_SOAK_SECS=180 cargo test -p logless-core --lib -- sync::soak    8.16s user 7.88s system 8% cpu 3:14.96 total
```
