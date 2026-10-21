# Spark Join Strategies — Deep-Dive Notes

> **Purpose**: How Spark executes joins — logical join types, the four physical join strategies Catalyst picks between, data skew handling, and how to diagnose and force strategies in the real pipeline. Project joins used as running examples: `load_warehouse` (fact rows ← `dim_company` / `dim_date` key resolution) and `build_gold_stock_enriched` (daily LEFT JOIN company_overview).
>
> **Prerequisites already covered**: execution plans, shuffle, repartition vs coalesce, data skew basics, AQE (see journey tracker Phase 4).
> **Last updated**: 2026-10-22

---

## 1. Why Joins Deserve a Deep Dive

A join is the most common source of **shuffle** — the most expensive Spark operation (network + disk + serialization). A pipeline's runtime is usually dominated by one or two joins. Knowing which *physical strategy* Spark chose for each join — and whether that was the right call — is the core of Spark performance work.

Cost intuition:

```text
no shuffle  <  shuffle small side  <  shuffle both sides  <  shuffle + sort both sides  <  cross product
(BHJ)           (SHJ-ish)              (SHJ / SMJ)           (SMJ)                        (BNLJ / cartesian)
```

---

## 2. Logical Join Types (what you ask for)

| Join | Returns | API |
| --- | --- | --- |
| Inner | rows with matches both sides | `df1.join(df2, key)` / `INNER JOIN` |
| Left outer | all left rows + matches | `df1.join(df2, key, "left")` |
| Right outer | all right rows + matches | `"right"` |
| Full outer | all rows both sides | `"outer"` / `FULL OUTER JOIN` |
| Left semi | left rows that HAVE a match (no right columns) | `"left_semi"` / `WHERE EXISTS` |
| Left anti | left rows with NO match | `"left_anti"` / `WHERE NOT EXISTS` |
| Cross | every pair | `df.crossJoin(df2)` / `CROSS JOIN` |

Underrated for ETL: **semi/anti** replace `DISTINCT`-after-inner-join and null-trap `NOT IN` patterns — cleaner AND faster (no wide shuffle payload). Example: symbols in `fact_daily_stock` that have no `dim_company` row = **anti join** on `symbol` — that is literally our orphan-key validation, done as a join.

---

## 3. The Four Physical Join Strategies (what Spark actually runs)

Catalyst's physical plan stage picks ONE of these per logical join:

### 3.1 Broadcast Hash Join (BHJ) — best case

* The **small** side is collected to the driver and **broadcast** to every executor; each executor builds a hash table of it and streams the big side locally. **Zero shuffle of the big side.**
* Triggered automatically when one side's estimated size ≤ `spark.sql.autoBroadcastJoinThreshold` (**default 10 MB**) and stats are available — which is exactly why `ANALYZE TABLE ... COMPUTE STATISTICS` matters (you already ran it on `silver.daily_stock_data`).
* Support: inner + all outer joins (outer requires broadcasting the side that may not match).

```python
from pyspark.sql import functions as F
facts.join(F.broadcast(dim_company), "symbol")          # PySpark hint
```

```sql
SELECT /*+ BROADCAST(dc) */ f.symbol, dc.sector
FROM fact_daily_stock f JOIN dim_company dc ON f.symbol = dc.symbol;
```

In the physical plan it shows as `BroadcastHashJoin`. Also watch `BroadcastExchange` — the driver-collect step; if the broadcast side is too big, driver memory dies (`spark.driver.maxResultSize`, OOM).

**In our warehouse**: `dim_company` (a few hundred rows) and `dim_date` (a few thousand) are always broadcast-eligible — the `load_warehouse` fact-to-dim key resolution joins are textbook BHJ candidates.

### 3.2 Shuffled Hash Join (SHJ)

* Both sides shuffled on the join key; the **smaller side's** partition is hashed into memory and probed by the larger side's partition. No sort needed.
* Faster than SMJ when one side is small *per partition* but too big to broadcast (>10 MB). **Off by default** — Spark prefers SMJ because SHJ needs one side to fit in memory per partition (spill risk). Enable preference: `spark.sql.join.preferSortMergeJoin = false`.
* Plan shows `ShuffledHashJoin`.

### 3.3 Sort-Merge Join (SMJ) — the default workhorse

* Both sides shuffled on the key, **both sorted by the key**, then a streaming merge. `SortMergeJoin` in the plan.
* Default choice for large equi-joins: **spillable and robust** (memory pressure degrades to disk, doesn't OOM), handles all join types and duplicates well.
* Costs: two shuffles + two sorts. If the "small" side is actually broadcast-sized, SMJ is a *missed optimization* — always check why it wasn't BHJ (missing stats, threshold too low, hint absent).
* Our `build_gold_stock_enriched` daily↔overview join can land here if sizes grow and stats are stale — verify in the profile.

### 3.4 Broadcast Nested Loop Join (BNLJ) / Cartesian

* The fallback for **non-equi joins** (no equality condition, e.g. `f.close BETWEEN o.low_52w AND o.high_52w`) and cross joins. O(M × N) comparisons.
* If you see `BroadcastNestedLoopJoin` on big inputs, it's usually a bug (accidental cross join, missing join condition, null-intolerant predicate). Fix the query, don't tune it.

---

## 4. Catalyst's Decision Tree

```text
Is the join an equi-join (equality on keys)?
├── NO  → Broadcast Nested Loop (or Cartesian if no condition at all)
└── YES → Does one side's estimated size ≤ broadcast threshold (10MB default)
           AND stats available?
           ├── YES → Broadcast Hash Join
           └── NO  → Is preferSortMergeJoin = false AND one side fits in
                     memory per partition?
                     ├── YES → Shuffled Hash Join
                     └── NO  → Sort-Merge Join

With AQE enabled (default in Databricks), the choice is re-planned at runtime
with REAL sizes: a planned SMJ can be demoted to BHJ mid-flight when the
small side turns out tiny — and skew handled automatically.
```

Key configs:

| Config | Default | Effect |
| --- | --- | --- |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | BHJ cutoff (bytes) |
| `spark.sql.join.preferSortMergeJoin` | true | SMJ vs SHJ preference |
| `spark.sql.adaptive.enabled` | true (Databricks) | AQE — runtime strategy switching |
| `spark.sql.adaptive.skewJoin.enabled` | true (with AQE) | automatic skew handling |
| `spark.sql.shuffle.partitions` | 200 | partitions per shuffle side (AQE coalesces down automatically) |

---

## 5. Forcing a Strategy (hints)

SQL hints: `/*+ BROADCAST(t) */`, `/*+ SHUFFLE_HASH(t) */`, `/*+ MERGE(t) */`, `/*+ SHUFFLE_REPLICATE_NL(t) */`. PySpark: `F.broadcast(df)`.

When to force:

* Dim↔fact join where the dim is known-small but stats are missing/stale → `BROADCAST`
* Repeated driver OOM from over-eager auto-broadcast → lower the threshold, not the hint
* Testing/learning: force SMJ on a dim join and watch the shuffle appear — this is how you internalize the cost model

Hints are advice, not orders: with AQE, a runtime-re-sized side can still overrule.

---

## 6. Data Skew in Joins

**Symptoms**: one straggler task while every other join task finished (Spark UI / query profile — one task with 10–100× the records); heavy spill to disk; stage stuck at 99%.

Classic causes in stock data: a **null join key** (all nulls hash to one partition), one mega-symbol dominating rows, or a placeholder/UNKNOWN member (from §5 of the DW notes) concentrating mass.

Fixes, in preference order:

1. **AQE skew join** (on by default in Databricks): splits oversize partitions of the skewed side into sub-partitions and replicates the matching partitions of the other side — no code change
2. **Broadcast the small/skewed-against side** — skew is irrelevant when there's no shuffle
3. **Filter or split the hot key** before the join
4. **Manual salting**: add a random 0..N key to the hot side, explode the other side to N copies, join on `(key, salt)`, then dedupe — last resort; document why
5. Null keys: `WHERE key IS NOT NULL` or join nulls separately as an anti-pattern check

---

## 7. Diagnosing Joins in Practice

1. `df.explain()` (or `%sql EXPLAIN ...` / the query profile) → find `BroadcastHashJoin` / `SortMergeJoin` / `ShuffledHashJoin` / `BroadcastNestedLoopJoin`
2. For each join ask: *should this be a BHJ?* (small side present?) → if it's SMJ, check stats (`DESCRIBE DETAIL`, size in bytes; run `ANALYZE ... COMPUTE STATISTICS`)
3. Query profile / Spark UI: shuffle bytes, task time distribution (skew!), spill (memory)
4. Fix one thing, re-profile — never tune on theory alone

```python
joined = facts.join(dim_company, "symbol")
joined.explain(True)   # look at the == Physical Plan == section
```

---

## 8. Application to Our Pipeline

| Join | Where | Expected strategy | Why |
| --- | --- | --- | --- |
| fact rows ← `dim_company` (resolve company_key) | `load_warehouse` | **BHJ** | dim is a few hundred rows; if you see SMJ, stats are missing — ANALYZE the dim and/or BROADCAST hint |
| fact rows ← `dim_date` (resolve date_key) | `load_warehouse` | **BHJ** | same — few thousand rows |
| daily LEFT JOIN company_overview | `build_gold_stock_enriched` | BHJ now, SMJ as overview grows | overview Silver is modest today; watch it when fundamentals history accumulates |
| MERGE INTO silver/gold | build_silver / build_gold | internal join on the merge key | MERGE's source↔target matching join — keep source deduped first (we do) |
| orphan-key validation | `load_warehouse` | anti-join semantics | textbook LEFT ANTI pattern |

Delta/UC extras: `spark.databricks.optimizer.delta.*` / dynamic file pruning skip whole files on join-filtered reads; **liquid clustering** on `(symbol, trade_date)` shrinks the scan feeding the join.

---

## 9. Best-Practice Checklist

* Equi-joins whenever semantically possible; never cross join by accident
* Broadcast dimensions; ANALYZE tables so Spark KNOWS they're small
* Prefer semi/anti joins over DISTINCT-after-join and `NOT IN`
* Filter and select narrow columns **before** joining (shuffle payload ↓)
* Watch shuffle partitions and AQE coalescing; don't hand-set 200-partition habits
* Null join keys: filter or handle explicitly
* Profile before and after every join change

---

## 10. Interview Questions

1. What join strategies does Spark have and how does Catalyst choose? (decision tree in §4)
2. Why is Sort-Merge the default for large joins when Shuffled Hash can be faster? (robustness/spill vs memory risk)
3. What does the broadcast threshold control, and what are the failure modes of broadcasting something too large? (driver OOM, `maxResultSize`)
4. What are the symptoms of a skewed join, and what are four remedies?
5. What does AQE change about join planning at runtime? (real sizes → demote SMJ→BHJ, skew split, partition coalescing)
6. Why does `ANALYZE TABLE ... COMPUTE STATISTICS` change join plans?
7. When would you see a Broadcast Nested Loop join, and what do you do about it?
8. Left semi vs inner join — what's the difference and when does semi win? (our orphan-key check)
9. How would you optimize a fact-to-dimension join of 1B × 10k rows? (broadcast, stats, pruning)
10. How does salting fix skew, and what's the cost? (replication N× of the other side + dedupe)

---

## 11. Practice Criteria (mastery ≠ notes)
- [ ] Run `EXPLAIN` on both `load_warehouse` dim joins — confirm `BroadcastHashJoin`; if SMJ, ANALYZE the dims and re-check
- [ ] Force SMJ with a hint on one dim join and observe the added shuffles in the query profile
- [ ] Find the strategy used by `build_gold_stock_enriched`'s daily↔overview join and defend it
- [ ] Deliberately skew a small test join (e.g., 90% one key), confirm AQE splits it, then fix with broadcast
- [ ] Rewrite the orphan-key validation as a LEFT ANTI join and compare against the current row-count check
- [ ] Answer all 10 interview questions from memory

---

*(Source: Spark join-strategy deep-dive for the journey tracker's next learning focus (2026-10-22); project joins from `load_warehouse`, `build_gold_stock_enriched`, and `etl.pipeline_watermarks` workflows as implemented 2026-10-16.)