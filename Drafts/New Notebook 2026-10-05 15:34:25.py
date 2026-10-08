# Databricks notebook source
# ============================================================
# 1. CREATE SAMPLE DATA
# ============================================================

employees = spark.createDataFrame(
    [
        (1, "Rahul", 101),
        (2, "Anna", 102),
        (3, "Ben", 103),
        (4, "John", 104),
        (5, "Priya", 105),
    ],
    ["emp_id", "name", "dept_id"]
)

departments = spark.createDataFrame(
    [
        (101, "Engineering"),
        (102, "Finance"),
        (103, "HR"),
        (106, "Marketing"),
    ],
    ["dept_id", "department"]
)


# ============================================================
# 2. SHOW INPUT DATA
# ============================================================

print("========== EMPLOYEES ==========")
employees.show()

print("========== DEPARTMENTS ==========")
departments.show()


# ============================================================
# 3. INNER JOIN
# ============================================================

print("\n========== INNER JOIN ==========")

inner_df = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "inner"
)

inner_df.show()

print("\n========== INNER JOIN PLAN ==========")

inner_df.explain("formatted")


# ============================================================
# 4. LEFT JOIN
# ============================================================

print("\n========== LEFT JOIN ==========")

left_df = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "left"
)

left_df.show()

print("\n========== LEFT JOIN PLAN ==========")

left_df.explain("formatted")


# ============================================================
# 5. FULL OUTER JOIN
# ============================================================

print("\n========== FULL OUTER JOIN ==========")

full_df = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "full"
)

full_df.show()

print("\n========== FULL JOIN PLAN ==========")

full_df.explain("formatted")


# ============================================================
# 6. DISABLE BROADCAST
# ============================================================
# The datasets are tiny, so Spark may choose BroadcastHashJoin.
# Disable automatic broadcasting to study other join strategies.

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    -1
)


# ============================================================
# 7. INNER JOIN WITH BROADCAST DISABLED
# ============================================================

print("\n========== INNER JOIN - BROADCAST DISABLED ==========")

inner_no_broadcast = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "inner"
)

inner_no_broadcast.show()

print("\n========== PHYSICAL PLAN ==========")

inner_no_broadcast.explain("formatted")


# ============================================================
# 8. CHECK SPARK CONFIGURATION
# ============================================================

print("\n========== SPARK CONFIGURATION ==========")

print(
    "Shuffle partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)

print(
    "Broadcast threshold:",
    spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
)