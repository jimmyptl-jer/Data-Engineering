Absolutely. This should become a **proper canonical note**, because Schema Handling is the foundation for everything we are about to do with the Massive endpoints.

# Schema Handling — Production Data Transformation Guide

> **Purpose:** Learn how to design, implement, and verify schemas for real production data — not simply learn `StructType` syntax.

---

# 1. What Is Schema Handling?

A schema defines the **expected structure of data**.

It tells Spark:

* What fields exist
* What each field is called
* What data type each field has
* Whether a field can be `NULL`
* How nested structures are organized
* What shape the DataFrame should have

In a production pipeline:

```text
Source
   ↓
Raw Response
   ↓
Schema Interpretation
   ↓
Transformation
   ↓
Canonical Schema
   ↓
Validation
   ↓
Silver
```

For our Massive pipeline:

```text
Massive API
     ↓
Raw JSON
     ↓
S3 Bronze
     ↓
Spark
     ↓
Schema Handling
     ↓
Transformation
     ↓
Silver
```

The important idea is:

> **Schema handling is not just writing `StructType`. It is deciding what the data means and what structure the pipeline should guarantee.**

---

# 2. Why Schema Matters

Without a controlled schema, Spark may infer the structure from the data it happens to see.

For example:

```python
.option("inferSchema", "true")
```

might produce:

```text
volume → IntegerType
```

today.

But tomorrow the data may cause Spark to infer:

```text
volume → LongType
```

or:

```text
volume → StringType
```

That can create inconsistent downstream behavior.

A production pipeline should instead establish:

```text
Expected Schema
       ↓
Actual Data
       ↓
Compare
       ↓
Accept / Reject / Investigate
```

---

# 3. Explicit Schema vs `inferSchema`

## `inferSchema`

Useful for:

* Exploration
* Learning
* Quick prototypes
* Unknown datasets

Example:

```python
df = (
    spark.read
    .json(path)
)
```

or:

```python
.option("inferSchema", "true")
```

The problem is that Spark is deciding the schema based on the data.

---

## Explicit Schema

Example:

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

stock_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])
```

Now the pipeline explicitly states:

> This is what we expect this dataset to look like.

---

# 4. The Three Schema Layers

We need to distinguish three schemas in our architecture.

```text
             SOURCE
                │
                ▼
        Source / Bronze Schema
                │
                ▼
        Canonical Silver Schema
                │
                ▼
             Gold Schema
```

They serve different purposes.

---

## 4.1 Bronze Schema

Bronze represents:

> **What did the source actually give us?**

The goal is to preserve the source representation as much as practical.

For example, Massive might provide:

```json
{
  "T": "AAPL",
  "o": 150.2,
  "h": 153.4,
  "l": 149.8,
  "c": 152.1,
  "v": 1200000,
  "t": 1754563200000
}
```

Bronze should not immediately turn this into an elaborate business model.

Bronze is primarily about:

* preservation
* replay
* lineage
* recoverability

---

# 5. Silver Schema

Silver answers:

> **What should our canonical representation of this data be?**

For example:

| Bronze | Silver             |
| ------ | ------------------ |
| `T`    | `ticker`           |
| `o`    | `open`             |
| `h`    | `high`             |
| `l`    | `low`              |
| `c`    | `close`            |
| `v`    | `volume`           |
| `t`    | `market_timestamp` |

The Silver layer should not force downstream users to understand the quirks of the Massive API.

That's one of its major purposes.

---

# 6. Gold Schema

Gold answers:

> **What structure does the business or analytical use case need?**

For example:

```text
Silver Prices
       +
Company Overview
       +
Dividends
       ↓
Gold Company Dataset
```

Gold may contain:

```text
ticker
company_name
sector
market_date
close
daily_return
market_cap
dividend
```

Gold is therefore **business-oriented**, rather than source-oriented.

---

# 7. The Most Important Question: What Is One Row?

Before defining a schema, identify the **grain**.

Grain means:

> **What does one row represent?**

This is more important than knowing `StructType`.

---

## Aggregates

Potential grain:

```text
1 row = 1 ticker + 1 trading interval
```

Example:

```text
AAPL | 2026-08-12 | 150.20 | 153.40 | ...
```

---

## Dividends

Potential grain:

```text
1 row = 1 dividend event
```

---

## Stock Overview

Potential grain:

```text
1 row = 1 company snapshot
```

---

## Exchanges

Potential grain:

```text
1 row = 1 exchange
```

These datasets therefore require **different schemas**.

---

# 8. Why Grain Comes Before Schema

Suppose we have:

```text
ticker
date
open
close
volume
```

We still don't know whether:

```text
1 row = daily stock
```

or:

```text
1 row = hourly stock
```

or:

```text
1 row = five-minute stock
```

The schema alone doesn't tell us the business meaning.

Therefore:

```text
Understand data
      ↓
Determine grain
      ↓
Determine business key
      ↓
Design schema
```

---

# 9. Business Key

After determining the grain, identify the **business key**.

For daily stock data:

```text
ticker + market_date
```

could represent the unique business record.

For another endpoint it might be:

```text
ticker + dividend_date
```

or:

```text
exchange_id
```

The business key is important for:

* deduplication
* incremental processing
* upsert
* MERGE
* corrections
* joins

---

# 10. Schema Design Process

This is our standard process for **every Massive endpoint**.

```text
STEP 1
Understand raw response
        ↓
STEP 2
Identify row grain
        ↓
STEP 3
Identify business key
        ↓
STEP 4
Identify source fields
        ↓
STEP 5
Define canonical names
        ↓
STEP 6
Choose Spark data types
        ↓
STEP 7
Define nullable/non-nullable expectations
        ↓
STEP 8
Define metadata columns
        ↓
STEP 9
Create explicit schema
        ↓
STEP 10
Test against real Bronze
```

This is our **schema engineering workflow**.

---

# 11. Step 1 — Understand the Raw Response

Before writing any Spark code, inspect the actual response.

We want to know:

```text
Is it:
    struct?
    array?
    map?
    array<struct>?
    nested struct?
```

For example:

```text
root
 ├── ticker: string
 └── results: array
       └── element: struct
             ├── timestamp
             ├── open
             ├── high
             ├── low
             ├── close
             └── volume
```

This immediately tells us that the transformation will likely involve:

```python
explode("results")
```

---

# 12. Step 2 — Identify Row Grain

Ask:

> What does one final Silver row represent?

For aggregates:

```text
ticker + trading interval
```

For dividends:

```text
one dividend event
```

For overview:

```text
one company snapshot
```

For exchanges:

```text
one exchange
```

Never skip this step.

---

# 13. Step 3 — Identify Business Key

Once the grain is understood, identify what uniquely identifies a record.

Example:

```text
ticker + market_date
```

This key later supports:

```text
Deduplication
     ↓
Incremental processing
     ↓
Upsert
     ↓
MERGE
```

---

# 14. Step 4 — Identify Source Fields

Create a field inventory.

Example:

| Source Field | Meaning       |
| ------------ | ------------- |
| `T`          | ticker        |
| `o`          | opening price |
| `h`          | highest price |
| `l`          | lowest price  |
| `c`          | closing price |
| `v`          | volume        |
| `t`          | timestamp     |

At this point we're **understanding**, not transforming.

---

# 15. Step 5 — Define Canonical Names

Source APIs often use poor or abbreviated names.

Example:

```text
T → ticker
o → open
h → high
l → low
c → close
v → volume
t → market_timestamp
```

Canonical naming gives us:

```text
API-specific representation
            ↓
     Internal standard
```

This is important because future sources may use completely different names.

For example:

```text
Massive → c
Alpha Vantage → close
CSV → closing_price
```

Our Silver representation can still be:

```text
close
```

---

# 16. Step 6 — Choose Spark Data Types

Types should reflect **business meaning**, not simply the source representation.

Typical stock schema:

```text
ticker             StringType
market_date        DateType
open               DoubleType
high               DoubleType
low                DoubleType
close              DoubleType
volume             LongType
```

For timestamps:

```text
TimestampType
```

For dates:

```text
DateType
```

For categorical values:

```text
StringType
```

For numeric measurements:

```text
DoubleType
DecimalType
LongType
```

The choice depends on precision and business requirements.

---

# 17. `DoubleType` vs `DecimalType`

This becomes important in financial data.

`DoubleType` is convenient:

```python
DoubleType()
```

but floating-point representation has precision limitations.

For financial calculations where exact decimal precision matters, we may eventually prefer:

```python
DecimalType(18, 4)
```

or another appropriate precision/scale.

We should therefore avoid blindly choosing `double` for every financial value.

For today's Massive work, we'll choose types based on the actual endpoint and downstream requirements.

---

# 18. Step 7 — Nullability

Schema fields can specify whether `NULL` is expected.

Example:

```python
StructField(
    "ticker",
    StringType(),
    False
)
```

means:

```text
ticker should not be NULL
```

Where:

```python
StructField(
    "description",
    StringType(),
    True
)
```

means:

```text
description may be NULL
```

But there's an important distinction:

> **Schema nullability expresses expectation; it is not a complete data-quality system.**

We still need validation.

---

# 19. Schema Handling vs Validation

Suppose:

```text
close = NULL
```

Schema tells us:

```text
close → DoubleType
```

Validation asks:

```text
Is NULL acceptable for close?
```

Therefore:

```text
Schema
  ↓
"What shape/type should this field have?"

Validation
  ↓
"Is the actual value acceptable?"
```

They work together but are not the same thing.

---

# 20. Step 8 — Metadata Columns

Our canonical Silver schema should also consider pipeline metadata.

Example:

```text
source
dataset_name
ticker
batch_id
run_id
ingestion_timestamp
source_file
source_path
pipeline_version
```

Conceptually:

```text
Business Data
      +
Pipeline Metadata
      ↓
Silver Contract
```

---

# 21. Why Metadata Matters

### Debugging

You can determine:

> Which run produced this record?

### Lineage

You can determine:

> Where did this record come from?

### Replay

You can determine:

> Which Bronze input should I process again?

### Auditing

You can determine:

> When did we receive this data?

### Production support

You can determine:

> Which pipeline execution generated this problem?

---

# 22. Ingestion Date vs Business Date

This distinction is critical.

Suppose:

```text
Market date:
2026-08-07
```

but we retrieve the data:

```text
2026-08-10
```

Then:

```text
market_date      = 2026-08-07
ingestion_date   = 2026-08-10
```

These must not be confused.

```text
Business time
      ≠
Pipeline time
```

This distinction becomes important for:

* incremental loading
* late-arriving data
* corrections
* partitioning
* replay
* auditing

---

# 23. Step 9 — Create Explicit Schema

Once all decisions are made, create the schema.

Example:

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType
)

aggregate_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("market_timestamp", TimestampType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
])
```

This is only the **schema definition**.

It doesn't prove the schema is correct.

---

# 24. Step 10 — Test Against Real Bronze

This is where we move from:

> **Implemented**

to potentially:

> **Production-Verified**

We need to actually read the Bronze data:

```text
S3 Bronze
    ↓
Explicit Schema
    ↓
Spark DataFrame
    ↓
printSchema()
    ↓
Inspect data
```

Then verify:

### Structure

Are the expected fields present?

### Types

Are the types correct?

### Grain

Does one row represent what we intended?

### Nullability

Are required fields populated?

### Business key

Can records be uniquely identified?

### Downstream compatibility

Does the transformation produce valid Silver data?

---

# 25. Schema Drift

Production APIs can change.

For example, Massive could introduce:

```text
new_field
```

or change:

```text
volume
```

from one representation to another.

We therefore need to distinguish:

```text
Expected Schema
       ↓
Actual Schema
```

and detect unexpected changes.

Schema drift can be:

### Additive

New field appears.

### Breaking

Existing field changes type.

### Structural

Nested structure changes.

### Semantic

Field still exists but its meaning changes.

The last one can be particularly dangerous because Spark may not necessarily fail.

---

# 26. Schema Handling for Massive Endpoints

Our Massive endpoints should be treated individually.

```text
Massive
│
├── Aggregates
│     └── Market data / OHLCV
│
├── Dividends
│     └── Corporate action events
│
├── Stock Overview
│     └── Company snapshot
│
└── Exchanges
      └── Reference/master data
```

They have different:

* schemas
* grains
* business keys
* transformations
* incremental strategies
* validation rules
* downstream uses

Therefore:

> **Do not create one giant schema for the entire Massive API.**

---

# 27. Aggregates — First Practical Case

We'll start with Aggregates because it teaches nested schema handling.

Conceptually:

```text
Massive Aggregates
        ↓
Response
        ↓
results[]
        ↓
array<struct>
        ↓
explode()
        ↓
one row per aggregate
        ↓
canonical schema
        ↓
Silver
```

Before writing anything, we inspect:

```python
df.printSchema()
```

and:

```python
df.show(truncate=False)
```

Then identify:

```text
1. Structure
2. Grain
3. Business key
4. Fields
5. Types
6. Nullability
7. Metadata
```

Only then do we create the explicit schema.

---

# 28. Schema Contract

For each endpoint, we'll maintain a mapping like:

| Source | Canonical Silver   | Spark Type    | Nullable | Meaning           |
| ------ | ------------------ | ------------- | -------- | ----------------- |
| `T`    | `ticker`           | StringType    | No       | Security symbol   |
| `t`    | `market_timestamp` | TimestampType | No       | Market event time |
| `o`    | `open`             | DoubleType    | Yes      | Opening price     |
| `h`    | `high`             | DoubleType    | Yes      | Highest price     |
| `l`    | `low`              | DoubleType    | Yes      | Lowest price      |
| `c`    | `close`            | DoubleType    | Yes      | Closing price     |
| `v`    | `volume`           | LongType      | Yes      | Traded volume     |

This table becomes the **contract between transformation and downstream consumers**.

---

# 29. Schema Handling Competency Levels

We will track schema handling using the same competency system as our 90-day tracker.

## Level 0 — Not Started

You haven't worked with the concept.

---

## Level 1 — Concept Understood

You can explain:

* schema
* field
* data type
* nullable
* nested schema
* explicit vs inferred schema
* row grain
* business key

without relying on memorized code.

---

## Level 2 — Briefly Touched

You've written or used:

```python
StructType(...)
StructField(...)
```

but haven't independently designed a production schema.

---

## Level 3 — Implemented

You can independently take:

```text
Raw Massive response
       ↓
Understand structure
       ↓
Determine grain
       ↓
Design schema
       ↓
Implement explicit schema
       ↓
Read Bronze
```

and make it work.

---

## Level 4 — Production-Verified

The schema has been proven against real production data.

We verify:

* expected fields
* correct types
* expected nullability
* correct grain
* correct business key
* correct metadata
* downstream transformation
* no unexpected schema drift

Only then do we call it:

> 🟢 **Production-Verified**

---

# 30. The Golden Rule

The most important rule for this section:

> **Never start by writing `StructType`. Start by understanding the data.**

The correct order is:

```text
Understand
    ↓
Determine grain
    ↓
Determine business key
    ↓
Map fields
    ↓
Choose types
    ↓
Define nullability
    ↓
Add metadata
    ↓
Create schema
    ↓
Test against Bronze
    ↓
Verify Silver
```

---

# 31. Our Actual Massive Schema Journey

We'll now apply this guide endpoint-by-endpoint:

```text
                 MASSIVE
                    │
        ┌───────────┼───────────┐
        ↓           ↓           ↓
   Aggregates    Dividends   Overview
        │           │           │
        ↓           ↓           ↓
     Schema       Schema      Schema
        │           │           │
        └───────────┼───────────┘
                    ↓
                 Silver
                    ↓
              Join / Merge
                    ↓
                  Gold
```

And separately:

```text
Exchanges
    ↓
Reference Silver
    ↓
Used by downstream joins
```

---

# 32. What We Have and What We Still Need to Prove

For our current project, we already have evidence that **Massive ingestion and Bronze/Silver processing are running**.

But we should **not automatically mark Schema Handling as production-verified for every endpoint**.

We still need to inspect the actual implementation and prove:

```text
Massive endpoint
      ↓
Explicit/controlled schema
      ↓
Correct row grain
      ↓
Correct types
      ↓
Correct metadata
      ↓
Correct transformation
      ↓
Correct Silver output
```

That distinction is exactly why we're using the competency ladder.

---

# 33. Practical Checklist

For every new Massive endpoint, we'll use this checklist:

```text
[ ] Understand raw response
[ ] Inspect printSchema()
[ ] Identify nested structures
[ ] Identify row grain
[ ] Identify business key
[ ] Inventory source fields
[ ] Map source → canonical names
[ ] Choose Spark types
[ ] Define nullability
[ ] Define metadata
[ ] Create explicit schema
[ ] Read real Bronze
[ ] Compare expected vs actual schema
[ ] Flatten/transform
[ ] Validate output
[ ] Verify Silver
[ ] Test downstream joins
[ ] Run production pipeline
[ ] Record evidence
```

### The key progression

```text
I know what StructType is
        ↓
I can write StructType
        ↓
I can design a schema
        ↓
I can implement it for a real endpoint
        ↓
I can prove it works in production
```

**That final step is what counts as completion in our 90-day competency tracker.**

---

## Today's practical boundary

We have now finished the **conceptual Schema Handling guide**.

We should **not move to the next transformation topic yet**.

Next, we should take the **actual Massive `aggregates` Bronze response/code** and walk through these 10 steps against real data.

That will turn today's learning from **"I understand schema design"** into **"I can independently design and implement a production schema."**
