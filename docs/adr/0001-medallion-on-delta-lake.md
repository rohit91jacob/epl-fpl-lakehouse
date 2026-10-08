# ADR 0001: Medallion architecture on Delta Lake with Spark

**Status:** accepted

## Context
The FPL API publishes mutable snapshots. Prices, availability and provisional bonus
points change during a gameweek, and history is revised until FPL sets `data_checked`.
We need reproducible analytics, the ability to replay from source, and a path to scale
beyond one machine.

## Decision
- Keep an immutable **raw** landing zone: every response byte for byte, plus a manifest
  with checksums.
- **Bronze** is an append-only Delta table with one row per response and its lineage.
- **Silver** holds typed, conformed tables keyed by `season`. They are written with MERGE
  (entities) or `replaceWhere` (facts derived wholesale from one resource).
- **Gold** is a dimensional model plus marts, rebuilt per season and swapped in
  atomically.
- Spark 4.1 runs in local mode by default. Pointing `EPL_SPARK_MASTER` at a cluster scales
  the same code.

## Consequences
- Any layer can be rebuilt from the layer below it, all the way back to the raw bytes.
- Delta adds ACID writes, time travel and schema evolution. The cost is a JVM and the
  Delta jars, which the Docker images bake in.
- For this data volume Spark is more than strictly needed. The trade buys
  production-grade semantics and a scale-out path.
