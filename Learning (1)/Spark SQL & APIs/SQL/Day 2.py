# Databricks notebook source
# MAGIC %md
# MAGIC ## 📘 HAVING — Complete Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What HAVING Actually Does
# MAGIC
# MAGIC HAVING is a **filter for groups**, not for rows.
# MAGIC
# MAGIC It runs after GROUP BY has already collapsed rows into groups, and decides which groups make it to the final output.
# MAGIC
# MAGIC ```
# MAGIC All rows
# MAGIC    ↓
# MAGIC WHERE filters rows
# MAGIC    ↓
# MAGIC GROUP BY forms groups
# MAGIC    ↓
# MAGIC HAVING filters groups     ← HAVING lives here
# MAGIC    ↓
# MAGIC SELECT computes output
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Basic Syntax
# MAGIC
# MAGIC ```sql
# MAGIC SELECT country, COUNT(*) AS total
# MAGIC FROM customers
# MAGIC GROUP BY country
# MAGIC HAVING COUNT(*) > 10;
# MAGIC ```
# MAGIC
# MAGIC Simple reading: *"Group by country, then only show me countries that have more than 10 customers."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## HAVING vs WHERE — The Most Important Distinction
# MAGIC
# MAGIC | | WHERE | HAVING |
# MAGIC |---|---|---|
# MAGIC | Filters | Rows | Groups |
# MAGIC | Runs | Before GROUP BY | After GROUP BY |
# MAGIC | Can use aggregates | ❌ No | ✅ Yes |
# MAGIC | Can use raw columns | ✅ Yes | ✅ Yes |
# MAGIC
# MAGIC ```sql
# MAGIC -- WHERE filters rows before grouping
# MAGIC SELECT country, SUM(revenue)
# MAGIC FROM orders
# MAGIC WHERE status = 'completed'      -- removes rows first
# MAGIC GROUP BY country;
# MAGIC
# MAGIC -- HAVING filters groups after grouping
# MAGIC SELECT country, SUM(revenue)
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) > 100000;   -- removes whole groups
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 1 — HAVING always needs GROUP BY
# MAGIC
# MAGIC HAVING without GROUP BY makes no sense in almost every real case.
# MAGIC
# MAGIC ```sql
# MAGIC -- ❌ Pointless — no groups formed
# MAGIC SELECT COUNT(*) FROM orders
# MAGIC HAVING COUNT(*) > 10;
# MAGIC
# MAGIC -- ✅ Correct
# MAGIC SELECT country, COUNT(*)
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING COUNT(*) > 10;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 2 — Can't use SELECT alias in HAVING
# MAGIC
# MAGIC HAVING runs before SELECT so the alias doesn't exist yet.
# MAGIC
# MAGIC ```sql
# MAGIC -- ❌ Fails
# MAGIC SELECT country, SUM(revenue) AS total
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING total > 100000;
# MAGIC
# MAGIC -- ✅ Correct — repeat the expression
# MAGIC SELECT country, SUM(revenue) AS total
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) > 100000;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 3 — HAVING can filter on a column not in SELECT
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Valid — filtering on COUNT but not showing it
# MAGIC SELECT country
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING COUNT(*) > 50;
# MAGIC ```
# MAGIC
# MAGIC The group still has a count — you're just choosing not to display it.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 4 — HAVING can use multiple conditions
# MAGIC
# MAGIC ```sql
# MAGIC SELECT country, SUM(revenue) AS total, COUNT(*) AS orders
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) > 100000
# MAGIC    AND COUNT(*) > 50
# MAGIC    AND AVG(revenue) > 200;
# MAGIC ```
# MAGIC
# MAGIC All conditions must be true for the group to appear.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 5 — WHERE + HAVING together (very common in DE)
# MAGIC
# MAGIC Use both to filter at two different levels.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     country,
# MAGIC     DATE_TRUNC('month', order_date)  AS month,
# MAGIC     SUM(revenue)                     AS total_revenue
# MAGIC FROM orders
# MAGIC WHERE status = 'completed'           -- row level filter first
# MAGIC GROUP BY country, month
# MAGIC HAVING SUM(revenue) > 50000          -- group level filter after
# MAGIC ORDER BY total_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC 👉 Think of it as: WHERE shrinks your data first, then HAVING filters which groups are meaningful.
# MAGIC
# MAGIC Always filter with WHERE where possible — it's faster because fewer rows reach GROUP BY.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 6 — HAVING can filter on non-aggregated columns too
# MAGIC
# MAGIC Though unusual, it's valid.
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Works but WHERE would be better here
# MAGIC SELECT country, COUNT(*)
# MAGIC FROM customers
# MAGIC GROUP BY country
# MAGIC HAVING country = 'India';
# MAGIC
# MAGIC -- ✅ Better — use WHERE instead
# MAGIC SELECT country, COUNT(*)
# MAGIC FROM customers
# MAGIC WHERE country = 'India'
# MAGIC GROUP BY country;
# MAGIC ```
# MAGIC
# MAGIC 👉 If you can write it in WHERE, always write it in WHERE. Faster and cleaner.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 7 — HAVING with BETWEEN, IN, LIKE
# MAGIC
# MAGIC All standard operators work in HAVING.
# MAGIC
# MAGIC ```sql
# MAGIC -- BETWEEN
# MAGIC SELECT country, SUM(revenue)
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) BETWEEN 50000 AND 200000;
# MAGIC
# MAGIC -- IN (less common but valid)
# MAGIC SELECT category, COUNT(*)
# MAGIC FROM products
# MAGIC GROUP BY category
# MAGIC HAVING COUNT(*) IN (10, 20, 30);
# MAGIC
# MAGIC -- Comparison operators
# MAGIC SELECT customer_id, COUNT(*) AS total_orders
# MAGIC FROM orders
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) >= 5;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Thumb Rule 8 — HAVING NULL behavior
# MAGIC
# MAGIC If the aggregate returns NULL, HAVING condition evaluates to unknown — which means the group is excluded.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT customer_id, SUM(revenue)
# MAGIC FROM orders
# MAGIC GROUP BY customer_id
# MAGIC HAVING SUM(revenue) > 0;
# MAGIC -- customers with all NULL revenues are silently excluded
# MAGIC ```
# MAGIC
# MAGIC Handle it safely:
# MAGIC
# MAGIC ```sql
# MAGIC HAVING COALESCE(SUM(revenue), 0) > 0
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Real DE Patterns Using HAVING
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 1 — Find duplicate records
# MAGIC
# MAGIC ```sql
# MAGIC -- Find customer_ids that appear more than once
# MAGIC SELECT customer_id, COUNT(*) AS occurrences
# MAGIC FROM customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC ```
# MAGIC
# MAGIC 👉 Most common deduplication check in pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 2 — Data quality check
# MAGIC
# MAGIC ```sql
# MAGIC -- Find dates that have abnormally low order counts
# MAGIC -- Could signal a pipeline failure that day
# MAGIC SELECT order_date, COUNT(*) AS daily_orders
# MAGIC FROM orders
# MAGIC GROUP BY order_date
# MAGIC HAVING COUNT(*) < 10
# MAGIC ORDER BY order_date;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 3 — Active customers only
# MAGIC
# MAGIC ```sql
# MAGIC -- Customers with at least 3 orders and total spend > $500
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*)        AS total_orders,
# MAGIC     SUM(revenue)    AS total_spend
# MAGIC FROM orders
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) >= 3
# MAGIC    AND SUM(revenue) > 500;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 4 — Finding inconsistent data
# MAGIC
# MAGIC ```sql
# MAGIC -- Products that have more than one price (data inconsistency)
# MAGIC SELECT product_id, COUNT(DISTINCT price) AS price_variants
# MAGIC FROM products
# MAGIC GROUP BY product_id
# MAGIC HAVING COUNT(DISTINCT price) > 1;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pattern 5 — Monthly pipeline health check
# MAGIC
# MAGIC ```sql
# MAGIC -- Find months where revenue dropped below threshold
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', order_date)  AS month,
# MAGIC     SUM(revenue)                     AS monthly_revenue,
# MAGIC     COUNT(*)                         AS total_orders
# MAGIC FROM orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY 1
# MAGIC HAVING SUM(revenue) < 100000
# MAGIC     OR COUNT(*) < 500
# MAGIC ORDER BY month;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Complete Picture — WHERE + GROUP BY + HAVING
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     COALESCE(country, 'Unknown')            AS country,
# MAGIC     DATE_TRUNC('month', order_date)         AS month,
# MAGIC     COUNT(*)                                AS total_orders,
# MAGIC     COUNT(DISTINCT customer_id)             AS unique_customers,
# MAGIC     SUM(revenue)                            AS total_revenue
# MAGIC FROM orders
# MAGIC WHERE status = 'completed'          -- 1. filter rows
# MAGIC GROUP BY 1, 2                       -- 2. form groups
# MAGIC HAVING SUM(revenue) > 10000         -- 3. filter groups
# MAGIC    AND COUNT(*) > 20                -- 4. second group filter
# MAGIC ORDER BY month DESC, total_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Reference Card
# MAGIC
# MAGIC | Rule | What to remember |
# MAGIC |------|-----------------|
# MAGIC | 1 | HAVING always pairs with GROUP BY |
# MAGIC | 2 | Can't use SELECT alias in HAVING |
# MAGIC | 3 | Can filter on columns not in SELECT |
# MAGIC | 4 | Multiple conditions with AND / OR |
# MAGIC | 5 | WHERE + HAVING together — use both |
# MAGIC | 6 | If it can go in WHERE, put it in WHERE |
# MAGIC | 7 | BETWEEN, IN, all operators work |
# MAGIC | 8 | NULLs in aggregates — use COALESCE |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3 Practice Problems
# MAGIC
# MAGIC **Problem 1 — Basic**
# MAGIC Find all product categories that have more than 100 orders and total revenue above $50,000.
# MAGIC
# MAGIC **Problem 2 — DE Level**
# MAGIC Find all customers who placed orders in at least 3 different months and spent more than $1,000 total. Show customer ID, number of months, and total spend.
# MAGIC
# MAGIC **Problem 3 — Data Quality**
# MAGIC Find all dates where the count of orders is less than the average daily order count. This simulates detecting a pipeline failure day.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Ready for **Day 3 — CASE WHEN** or want solutions to these 3 problems first?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📘 How SQL Query Actually Executes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Big Misunderstanding
# MAGIC
# MAGIC Most people think SQL executes in the order they **write** it.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT       ← you write this first
# MAGIC FROM         ← then this
# MAGIC WHERE        ← then this
# MAGIC GROUP BY     ← then this
# MAGIC HAVING       ← then this
# MAGIC ORDER BY     ← then this
# MAGIC LIMIT        ← then this
# MAGIC ```
# MAGIC
# MAGIC But the database executes it in a **completely different order.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Actual Execution Order
# MAGIC
# MAGIC ```
# MAGIC 1. FROM
# MAGIC 2. WHERE
# MAGIC 3. GROUP BY
# MAGIC 4. HAVING
# MAGIC 5. SELECT
# MAGIC 6. ORDER BY
# MAGIC 7. LIMIT
# MAGIC ```
# MAGIC
# MAGIC Let's go through each step with one single query so you can see exactly what happens at every stage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Query We'll Track
# MAGIC
# MAGIC ```sql
# MAGIC SELECT country, SUM(revenue) AS total_revenue
# MAGIC FROM orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) > 100000
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1 — FROM
# MAGIC
# MAGIC **What happens:** Database goes and fetches the full table first.
# MAGIC
# MAGIC ```
# MAGIC orders table — all rows loaded:
# MAGIC
# MAGIC id  | country | revenue | status
# MAGIC ----|---------|---------|----------
# MAGIC 1   | India   | 500     | completed
# MAGIC 2   | USA     | 300     | pending
# MAGIC 3   | India   | 700     | completed
# MAGIC 4   | UK      | 200     | completed
# MAGIC 5   | USA     | 900     | completed
# MAGIC 6   | India   | 100     | cancelled
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC Nothing filtered yet. Everything is available.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2 — WHERE
# MAGIC
# MAGIC **What happens:** Filters rows **before** any grouping happens.
# MAGIC
# MAGIC ```sql
# MAGIC WHERE status = 'completed'
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Rows that survive:
# MAGIC
# MAGIC id  | country | revenue | status
# MAGIC ----|---------|---------|----------
# MAGIC 1   | India   | 500     | completed
# MAGIC 3   | India   | 700     | completed
# MAGIC 4   | UK      | 200     | completed
# MAGIC 5   | USA     | 900     | completed
# MAGIC ...
# MAGIC
# MAGIC Row 2 (pending) and Row 6 (cancelled) are GONE.
# MAGIC ```
# MAGIC
# MAGIC 👉 This is why WHERE is powerful — it shrinks data **before** the heavy work of grouping starts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3 — GROUP BY
# MAGIC
# MAGIC **What happens:** Remaining rows are bucketed into groups.
# MAGIC
# MAGIC ```sql
# MAGIC GROUP BY country
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC India bucket  → [row1(500), row3(700), ...]
# MAGIC USA bucket    → [row5(900), ...]
# MAGIC UK bucket     → [row4(200), ...]
# MAGIC ```
# MAGIC
# MAGIC Each bucket will collapse into **one row**. Individual rows no longer exist after this step.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4 — HAVING
# MAGIC
# MAGIC **What happens:** Filters out entire groups that don't meet the condition.
# MAGIC
# MAGIC ```sql
# MAGIC HAVING SUM(revenue) > 100000
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC India bucket  → SUM = 450000  ✅ survives
# MAGIC USA bucket    → SUM = 80000   ❌ removed
# MAGIC UK bucket     → SUM = 120000  ✅ survives
# MAGIC ```
# MAGIC
# MAGIC USA group is completely gone from results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5 — SELECT
# MAGIC
# MAGIC **What happens:** Only NOW does the database compute what you asked for.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT country, SUM(revenue) AS total_revenue
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC country  | total_revenue
# MAGIC ---------|-------------
# MAGIC India    | 450000
# MAGIC UK       | 120000
# MAGIC ```
# MAGIC
# MAGIC The alias `total_revenue` is created **here** — which is why you can't use it in WHERE or HAVING. Those steps already ran before SELECT.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6 — ORDER BY
# MAGIC
# MAGIC **What happens:** Results are sorted.
# MAGIC
# MAGIC ```sql
# MAGIC ORDER BY total_revenue DESC
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC country  | total_revenue
# MAGIC ---------|-------------
# MAGIC India    | 450000        ← highest first
# MAGIC UK       | 120000
# MAGIC ```
# MAGIC
# MAGIC 👉 ORDER BY **can** use SELECT aliases because it runs after SELECT.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 7 — LIMIT
# MAGIC
# MAGIC **What happens:** Cuts the result to the number of rows you want.
# MAGIC
# MAGIC ```sql
# MAGIC LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC Since we only have 2 rows here, both show up. If there were 100 countries, only top 5 would appear.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Full Picture in One Visual
# MAGIC
# MAGIC ```
# MAGIC FROM orders
# MAGIC │
# MAGIC │  full table — all rows
# MAGIC ▼
# MAGIC WHERE status = 'completed'
# MAGIC │
# MAGIC │  cancelled + pending rows removed
# MAGIC ▼
# MAGIC GROUP BY country
# MAGIC │
# MAGIC │  rows bucketed per country
# MAGIC │  individual rows gone
# MAGIC ▼
# MAGIC HAVING SUM(revenue) > 100000
# MAGIC │
# MAGIC │  small revenue groups removed
# MAGIC ▼
# MAGIC SELECT country, SUM(revenue) AS total_revenue
# MAGIC │
# MAGIC │  columns computed, alias created
# MAGIC ▼
# MAGIC ORDER BY total_revenue DESC
# MAGIC │
# MAGIC │  sorted
# MAGIC ▼
# MAGIC LIMIT 5
# MAGIC │
# MAGIC │  cut to 5 rows
# MAGIC ▼
# MAGIC Final output
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why This Order Matters — Real Errors Explained
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 1 — Using SELECT alias in WHERE**
# MAGIC ```sql
# MAGIC -- ❌ Fails — WHERE runs before SELECT, alias doesn't exist yet
# MAGIC SELECT revenue * 2 AS doubled
# MAGIC FROM orders
# MAGIC WHERE doubled > 1000;
# MAGIC
# MAGIC -- ✅ Fix — repeat the expression
# MAGIC SELECT revenue * 2 AS doubled
# MAGIC FROM orders
# MAGIC WHERE revenue * 2 > 1000;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 2 — Using aggregate in WHERE**
# MAGIC ```sql
# MAGIC -- ❌ Fails — WHERE runs before GROUP BY, no aggregates exist yet
# MAGIC SELECT country, COUNT(*)
# MAGIC FROM orders
# MAGIC WHERE COUNT(*) > 10
# MAGIC GROUP BY country;
# MAGIC
# MAGIC -- ✅ Fix — use HAVING
# MAGIC SELECT country, COUNT(*)
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING COUNT(*) > 10;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 3 — Using SELECT alias in HAVING**
# MAGIC ```sql
# MAGIC -- ❌ Fails — HAVING runs before SELECT
# MAGIC SELECT country, SUM(revenue) AS total
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING total > 100000;
# MAGIC
# MAGIC -- ✅ Fix — repeat the expression
# MAGIC SELECT country, SUM(revenue) AS total
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC HAVING SUM(revenue) > 100000;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 4 — Using SELECT alias in ORDER BY** ✅ This one works
# MAGIC ```sql
# MAGIC -- ✅ Works fine — ORDER BY runs after SELECT
# MAGIC SELECT country, SUM(revenue) AS total
# MAGIC FROM orders
# MAGIC GROUP BY country
# MAGIC ORDER BY total DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Alias Availability Map
# MAGIC
# MAGIC ```
# MAGIC Step        | Can use SELECT alias?
# MAGIC ------------|---------------------
# MAGIC FROM        | ❌ No
# MAGIC WHERE       | ❌ No
# MAGIC GROUP BY    | ❌ No  (some DBs allow it, not standard)
# MAGIC HAVING      | ❌ No
# MAGIC SELECT      | ✅ Created here
# MAGIC ORDER BY    | ✅ Yes
# MAGIC LIMIT       | ✅ Yes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One Line to Remember Each Step
# MAGIC
# MAGIC | Step | One line |
# MAGIC |------|----------|
# MAGIC | FROM | Get the table |
# MAGIC | WHERE | Remove bad rows |
# MAGIC | GROUP BY | Bucket remaining rows |
# MAGIC | HAVING | Remove bad buckets |
# MAGIC | SELECT | Compute what to show |
# MAGIC | ORDER BY | Sort the output |
# MAGIC | LIMIT | Cut the output |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The One Sentence Summary
# MAGIC
# MAGIC > SQL is written in human reading order but executed in data processing order — FROM gets data, WHERE cleans it, GROUP BY organizes it, HAVING filters it, SELECT shapes it, ORDER BY sorts it, LIMIT cuts it.
# MAGIC
# MAGIC Memorize the execution order and 90% of SQL confusion disappears.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC You're right, my bad. Here it is as clean notes — no HTML, no coding.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Day 2 — Ultimate SQL Guide
# MAGIC ## GROUP BY · HAVING · COUNT · Execution Order · Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1. GROUP BY
# MAGIC
# MAGIC ## What it does
# MAGIC Collapses many rows into one row per group. Think of it as: *"For each X, give me one result row."*
# MAGIC
# MAGIC ## The Golden Rule
# MAGIC Every column in SELECT must either be inside an aggregate function OR listed in GROUP BY. No exceptions.
# MAGIC
# MAGIC ## All 12 Thumb Rules
# MAGIC
# MAGIC **Rule 1** — Every SELECT column must be aggregated or in GROUP BY
# MAGIC
# MAGIC **Rule 2** — GROUP BY column doesn't need to appear in SELECT
# MAGIC
# MAGIC **Rule 3** — Multiple columns = one row per unique combination of all those columns
# MAGIC
# MAGIC **Rule 4** — Can use position numbers (GROUP BY 1, 2) instead of repeating column names
# MAGIC
# MAGIC **Rule 5** — NULL forms its own group — use COALESCE to handle it cleanly
# MAGIC
# MAGIC **Rule 6** — WHERE runs before GROUP BY — always filter with WHERE first
# MAGIC
# MAGIC **Rule 7** — HAVING runs after GROUP BY — use it to filter groups not rows
# MAGIC
# MAGIC **Rule 8** — Cannot use SELECT alias in HAVING — repeat the full expression
# MAGIC
# MAGIC **Rule 9** — Can GROUP BY expressions, not just raw column names (e.g. DATE_TRUNC, LOWER)
# MAGIC
# MAGIC **Rule 10** — ORDER BY can use SELECT alias, GROUP BY cannot
# MAGIC
# MAGIC **Rule 11** — GROUP BY is DISTINCT + aggregation power combined
# MAGIC
# MAGIC **Rule 12** — GROUP BY does NOT sort results — always add ORDER BY explicitly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2. HAVING
# MAGIC
# MAGIC ## What it does
# MAGIC Filters groups after GROUP BY has already run. WHERE removes rows. HAVING removes entire groups.
# MAGIC
# MAGIC ## WHERE vs HAVING in one sentence
# MAGIC WHERE filters rows before grouping. HAVING filters groups after grouping.
# MAGIC
# MAGIC ## All 8 Thumb Rules
# MAGIC
# MAGIC **Rule 1** — HAVING always pairs with GROUP BY
# MAGIC
# MAGIC **Rule 2** — Cannot use SELECT alias in HAVING — HAVING runs before SELECT
# MAGIC
# MAGIC **Rule 3** — Can filter on a column that isn't even in SELECT
# MAGIC
# MAGIC **Rule 4** — Multiple conditions work — AND, OR, BETWEEN, IN all valid
# MAGIC
# MAGIC **Rule 5** — Use WHERE + HAVING together for two-level filtering — both in same query
# MAGIC
# MAGIC **Rule 6** — If the condition can go in WHERE, always put it in WHERE — it's faster
# MAGIC
# MAGIC **Rule 7** — BETWEEN, IN, all comparison operators work exactly the same as in WHERE
# MAGIC
# MAGIC **Rule 8** — If aggregate can return NULL, wrap with COALESCE to avoid silent exclusions
# MAGIC
# MAGIC ## Real DE Patterns
# MAGIC - Find duplicate records — HAVING COUNT(*) > 1
# MAGIC - Detect pipeline failure days — HAVING COUNT(*) < expected threshold
# MAGIC - Find inconsistent pricing — HAVING COUNT(DISTINCT price) > 1
# MAGIC - Active customers only — HAVING COUNT(*) >= 3 AND SUM(revenue) > 500
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3. COUNT(*) vs COUNT(column)
# MAGIC
# MAGIC COUNT(*) — counts every row including rows with NULLs. If the row exists, it gets counted.
# MAGIC
# MAGIC COUNT(column) — counts only rows where that specific column is NOT NULL. Skips NULLs silently.
# MAGIC
# MAGIC COUNT(DISTINCT column) — counts only unique non-NULL values in that column.
# MAGIC
# MAGIC ## Simple rule to remember
# MAGIC The * means the whole row. If the row exists, it counts. The column name means only count where that value exists.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 4. SQL Execution Order
# MAGIC
# MAGIC This is the actual order the database processes your query — not the order you write it.
# MAGIC
# MAGIC **Step 1 — FROM** — Get the full table. All rows loaded. Nothing filtered yet.
# MAGIC
# MAGIC **Step 2 — WHERE** — Filter rows before any grouping. Removes bad rows early. Makes everything after it faster.
# MAGIC
# MAGIC **Step 3 — GROUP BY** — Bucket remaining rows into groups. Individual rows collapse into one row per bucket.
# MAGIC
# MAGIC **Step 4 — HAVING** — Filter groups. Removes entire buckets that don't meet the condition.
# MAGIC
# MAGIC **Step 5 — SELECT** — Compute output columns. This is where aliases are created.
# MAGIC
# MAGIC **Step 6 — ORDER BY** — Sort the results. Can use SELECT aliases because SELECT already ran.
# MAGIC
# MAGIC **Step 7 — LIMIT** — Cut results to N rows.
# MAGIC
# MAGIC ## Alias availability map
# MAGIC
# MAGIC - FROM → alias does not exist
# MAGIC - WHERE → alias does not exist
# MAGIC - GROUP BY → alias does not exist
# MAGIC - HAVING → alias does not exist
# MAGIC - SELECT → alias is CREATED here
# MAGIC - ORDER BY → alias available
# MAGIC - LIMIT → alias available
# MAGIC
# MAGIC ## Why this matters — 3 common errors explained
# MAGIC
# MAGIC **Error 1** — Using aggregate in WHERE fails because WHERE runs before GROUP BY. No aggregates exist yet. Use HAVING instead.
# MAGIC
# MAGIC **Error 2** — Using SELECT alias in WHERE or HAVING fails because both run before SELECT. Repeat the full expression instead.
# MAGIC
# MAGIC **Error 3** — Using SELECT alias in ORDER BY works fine because ORDER BY runs after SELECT.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 5. Best Practices
# MAGIC
# MAGIC ## Writing clean SQL
# MAGIC - Never use SELECT * — if someone adds a column it silently breaks your pipeline
# MAGIC - One clause per line, consistent indentation
# MAGIC - Uppercase keywords, lowercase column names
# MAGIC - Meaningful aliases that describe what the column actually is
# MAGIC - Always use CTEs over deeply nested subqueries
# MAGIC
# MAGIC ## Performance
# MAGIC - Filter with WHERE before grouping — less data reaching GROUP BY
# MAGIC - Never wrap indexed columns in functions inside WHERE
# MAGIC - Pull only the columns you need in SELECT
# MAGIC
# MAGIC ## NULL handling
# MAGIC - Always use IS NULL not = NULL
# MAGIC - Use COALESCE to give NULLs a safe default
# MAGIC - In WHERE, remember != does not catch NULLs — add OR column IS NULL
# MAGIC
# MAGIC ## JOIN safety
# MAGIC - Always specify JOIN type explicitly — never implicit joins
# MAGIC - Check row counts before and after a JOIN — silent row multiplication is a common DE bug
# MAGIC - LEFT JOIN when you want to keep all records from the left table
# MAGIC
# MAGIC ## Data engineering specific
# MAGIC - Every pipeline query must be idempotent — running it twice should not duplicate data
# MAGIC - Always add updated_at timestamps to pipeline tables
# MAGIC - Run data quality checks after every load — NULL counts, uniqueness, row counts, date ranges
# MAGIC - Think in sets not rows — avoid row by row logic, use set based operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Quick Reference — One Line Each
# MAGIC
# MAGIC | Concept | One line |
# MAGIC |---------|----------|
# MAGIC | GROUP BY | Collapses rows into one row per group |
# MAGIC | HAVING | Filters groups after GROUP BY |
# MAGIC | WHERE vs HAVING | WHERE = rows, HAVING = groups |
# MAGIC | COUNT(*) | Counts all rows including NULLs |
# MAGIC | COUNT(col) | Counts only non-NULL values |
# MAGIC | Execution order | FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT |
# MAGIC | Alias in WHERE | Not allowed — SELECT hasn't run yet |
# MAGIC | Alias in ORDER BY | Allowed — SELECT already ran |
# MAGIC | NULL in GROUP BY | Forms its own group |
# MAGIC | Idempotency | Running twice = same result, no duplicates |
# MAGIC
# MAGIC ---