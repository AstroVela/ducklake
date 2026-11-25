# Maintenance policy and backlog query decision

The maintenance worker reads its policy and its live build and generation
backlog in separate statements within one metadata transaction. The November 3,
2026 controlled A/B experiment evaluated combining those reads. PR #74 withdrew
the combined query under the retention rule below.

## Historical comparison

Both query shapes used the same metadata and task inputs on the earlier stack.
The fixtures contained 14, 518, or 4,102 snapshots. Each size covered a live
backlog of two builds and one retired generation and an empty backlog with
stale persisted counts of seven and nine. An ordinary read transaction stayed
open during measurement. The probes checked the live counts, maintenance
budget, and source collection deferral.

The measured read probe ran from metadata connection setup through connection
close, including policy and backlog reads, source readiness, and commit. Each
case ran 16 counterbalanced ABBA/BAAB rounds with ten probes per block. The
decision used 3,840 primary probes measured without diagnostic query callbacks. A separate diagnostic pass
verified the policy and backlog statement counts.

## Retention rule

For each round, divide the combined query shape's total read-probe time by the
split shape's total. The benchmark required every medium and large case to
satisfy all three criteria.

1. A median paired-round ratio at most 0.98, equivalent to at least a 2% gain.
2. An upper bound below 1.0 for the descriptive 95% paired-bootstrap interval.
3. A ratio below 1.0 in at least 80% of rounds, which requires 13 of 16 wins.

Both small cases also had to stay within a 2% increase in the median ratio.
The benchmark wrote these thresholds before measurement and used 10,000
bootstrap resamples with seed 74174.

## Recorded results

| Snapshots | Backlog | Combined/split median ratio | 95% bootstrap interval | Combined wins out of 16 |
| ---: | --- | ---: | --- | ---: |
| 14 | Live | 1.001144 | 0.992646–1.015264 | 7 |
| 14 | Stale | 0.998597 | 0.993428–1.005347 | 9 |
| 518 | Live | 0.992215 | 0.981885–1.007151 | 10 |
| 518 | Stale | 0.987383 | 0.980938–0.999376 | 12 |
| 4,102 | Live | 0.993383 | 0.986044–1.002563 | 10 |
| 4,102 | Stale | 0.998316 | 0.990908–1.031675 | 9 |

All four medium and large cases fell short of the practical gain threshold.
Their median gains ranged from 0.17% to 1.26%. The 518-snapshot stale case
cleared the interval criterion and won twelve rounds.

## Archived records

- Measurement setup, detailed timings, reproduction commands, and artifact
  hashes. [Controlled A/B report at `2a937706`](https://github.com/AstroVela/ducklake/blob/2a9377062cddc0c222d8dd99e4d5efee3a298c76/docs/native-vortex-maintenance-probe-ab.md)
- Benchmark and regression sources. [Original test tree at `2a937706`](https://github.com/AstroVela/ducklake/tree/2a9377062cddc0c222d8dd99e4d5efee3a298c76/test/vane)
- Thirty-minute observation of the original combined-query candidate. [Historical report at `80f4110a`](https://github.com/AstroVela/ducklake/blob/80f4110ac45c555fbcd29e5dab52d0059983a339/docs/native-vortex-maintenance-probe-query.md)
