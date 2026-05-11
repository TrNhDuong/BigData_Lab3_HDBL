# Task 2-2: SKU Monthly Amount Standard Deviation with Dynamic Percentile Filtering

## Description
For each SKU within each month, compute the **standard deviation** of the amount of orders whose number of promotions meets a dynamic percentile threshold (P90 and P80).

**Conditions & Logic:**
- **Promotion Count**: Total number of promotion identifiers associated with an order.
- **Percentile Levels**: 
    - **P90**: Orders with promotion counts $\ge$ 90th percentile within the SKU-month group.
    - **P80**: Orders with promotion counts $\ge$ 80th percentile within the SKU-month group.
- **Edge Case**: If a group contains fewer than 2 qualifying orders after filtering, the standard deviation is set to **0.0**.
- **Implementation**: Contrast Spark's built-in `approx_percentile` with a self-implemented exact percentile using `cume_dist()`.

---

## How to Run

### Option A: Local mode with `spark-submit`
```bash
# 1. Build fat JAR
cd src/Task2-2
sbt assembly

# 2. Submit to Spark
spark-submit \
  --class Task22 \
  --master local[*] \
  target/scala-2.12/Task2-2-assembly-1.0.jar \
  "path/to/Amazon Sale Report.csv"
```

### Option B: Run directly with `sbt run`
```bash
cd src/Task2-2
sbt "run \"path/to/Amazon Sale Report.csv\""
```

---

## Output Schema
The result is saved as a single PARQUET file `Task_2-2.parquet`:

| Column | Type | Description |
|---|---|---|
| `SKU` | String | Stock Keeping Unit |
| `year` | Int | Year of the order |
| `month` | Int | Month of the order |
| `std_p90_approx` | Double | StdDev using approximate P90 threshold |
| `std_p80_approx` | Double | StdDev using approximate P80 threshold |
| `std_p90_exact` | Double | StdDev using exact P90 threshold |
| `std_p80_exact` | Double | StdDev using exact P80 threshold |

---

## Algorithm (Spark Structured API Steps)

1. **Preprocessing**: Parse dates, extract year/month, and calculate `promo_count` by splitting the `promotion-ids` string.
2. **Threshold Calculation (Approx)**: Use `percentile_approx` to get P90/P80 values per group.
3. **Threshold Calculation (Exact)**: Use `cume_dist()` window function to find the exact threshold where ECDF $\ge 0.90$ or $0.80$.
4. **Filtering & Aggregation**: Join thresholds back to the main data, filter rows, and apply `stddev_pop`.
5. **Benchmarking**: Repeat the calculation 5 times to compute mean execution time and standard deviation.
6. **Export**: Coalesce to 1 partition and write as Parquet.
