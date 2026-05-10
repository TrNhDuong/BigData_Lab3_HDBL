# Task 1-1: Sliding Window — Most Bought Size per State

## Description
For each date `d` and each state, find the **size most frequently bought** within the 7-day window `[d-7, d-1]`.

**Conditions for an order to be "bought":**
- `Status` contains `"Shipped"` (case-insensitive)
- `Qty > 0`

The window slides 1 day at a time; dates with no prior orders still appear in the output.

---

## How to Run

### Option A: Local mode with `spark-submit`
```bash
# 1. Build fat JAR
cd src/Task1-1
sbt assembly

# 2. Submit to Spark
spark-submit \
  --class Task1_1 \
  --master local[*] \
  target/scala-2.12/Task1-1-SlidingWindow-assembly-1.0.jar \
  "path/to/Amazon Sale Report.csv" \
  "output/Task1_1"
```

### Option B: Run directly with `sbt run`
```bash
cd src/Task1-1
sbt "run \"path/to/Amazon Sale Report.csv\" \"output/Task1_1\""
```

---

## Output Schema
The result is saved as a single CSV file at the specified output path:

| Column | Type | Description |
|---|---|---|
| `window_date` | Date (yyyy-MM-dd) | The sliding window anchor date `d` |
| `state` | String | Ship state (upper-case) |
| `most_bought_size` | String | Size with highest purchase count in window |
| `purchase_count` | Long | Number of qualifying orders for that size |

---

## Algorithm (MapReduce Steps)

1. **Load & Filter**: Read CSV, keep rows where `status` contains `"shipped"` and `qty > 0`.
2. **Map**: For each `(orderDate, state, size)` record, emit to every window date `d` where `orderDate ∈ [d-7, d-1]`.
3. **Reduce (Count)**: Sum counts per `(windowDate, state, size)`.
4. **Reduce (Max)**: Per `(windowDate, state)`, find the size with the maximum count.
5. **Export**: Write result as a single CSV file.
