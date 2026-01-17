# StarRocks

[StarRocks](https://www.starrocks.io/) is a modern analytical database product based on an MPP architecture. It provides real-time analytical capabilities, supporting both high-concurrency point queries and high-throughput complex analysis.

SQLMesh connects to StarRocks through its MySQL-compatible protocol and takes advantage of StarRocks-specific features like table models, partitioning, and distribution.

## Local/Built-in Scheduler

**Engine Adapter Type**: `starrocks`

### Installation

```
pip install "sqlmesh[starrocks]"
```

## Connection options

StarRocks uses the MySQL protocol for connections. Therefore, the connection parameters are similar to [MySQL](./mysql.md).

| Option             | Description                                                      |  Type  | Required |
|--------------------|------------------------------------------------------------------|:------:|:--------:|
| `type`             | Engine type name - must be `starrocks`                           | string |    Y     |
| `host`             | The hostname of the StarRocks FE (Frontend) server               | string |    Y     |
| `user`             | The username to use for authentication with the StarRocks server | string |    Y     |
| `password`         | The password to use for authentication with the StarRocks server | string |    Y     |
| `port`             | The port number of the StarRocks server (default: 9030)          | int    |    N     |
| `database`         | The target database                                              | string |    N     |
| `charset`          | The character set used for the connection                        | string |    N     |
| `collation`        | The collation used for the connection                            | string |    N     |
| `ssl_disabled`     | Whether SSL is disabled                                          | bool   |    N     |
| `concurrent_tasks` | The maximum number of concurrent tasks              | int    |    N     |

## Model Configuration

### Model Properties

You can configure StarRocks-specific behavior using these [model properties](../../concepts/models/overview.md#model-properties):

### Physical Properties

The StarRocks adapter recognizes the following [physical_properties](../../concepts/models/overview.md#physical_properties):

| Property         | Description                                                              |
|------------------|--------------------------------------------------------------------------|
| `primary_key`    | Primary key columns for PRIMARY KEY tables. If not specified, defaults to a DUPLICATE KEY table. |
| `distributed_by` | Data distribution across nodes. Use `HASH(columns := ..., buckets := N)` or `RANDOM(buckets := N)`, where buckets argument is optional. Defaults to RANDOM distribution. |
| `order_by`       | Sort key columns for physical data ordering on disk. |
| `rollup`         | Pre-aggregated indexes to accelerate aggregation queries. |

### Example

This model creates a PRIMARY KEY table optimized for analytical queries on order data:

```sql
MODEL (
  name analytics.daily_orders,
  kind FULL,
  physical_properties (
    primary_key = (order_date, order_id),
    distributed_by = HASH(columns := customer_id, buckets := 16),
    order_by = (order_date, customer_id),
    rollup = (
      rollup_by_customer(customer_id, order_date, total_amount),
      rollup_by_product(product_id, order_date, quantity)
    )
  )
);

SELECT
  order_id,
  order_date,
  customer_id,
  product_id,
  quantity,
  total_amount
FROM raw.orders
WHERE status = 'completed'
```

This generates a CREATE TABLE statement like:

```sql
CREATE TABLE analytics.daily_orders (
  order_id BIGINT,
  order_date DATE,
  customer_id BIGINT,
  ...
)
PRIMARY KEY (`order_date`, `order_id`)
DISTRIBUTED BY HASH (`customer_id`) BUCKETS 16
ORDER BY (`order_date`, `customer_id`)
ROLLUP (
  rollup_by_customer (`customer_id`, `order_date`, `total_amount`),
  rollup_by_product (`product_id`, `order_date`, `quantity`)
)
```

### Insert Overwrite Behavior

SQLMesh uses StarRocks' native `INSERT OVERWRITE` syntax for all models. This is because StarRocks has strict limitations on `DELETE` statements — for example, WHERE clauses don't support expressions like `CAST()` on DUPLICATE KEY tables.

SQLMesh automatically enables `dynamic_overwrite` for each connection session to ensure `INSERT OVERWRITE` only replaces partitions matching the source query data. This feature requires StarRocks v3.4.0 or later.

For more information, see the [StarRocks Dynamic overwrite documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#dynamic-overwrite).

## Limitations

- **No transactions** — StarRocks supports only limited number of use cases for transactions, so SQLMesh runs operations non-transactionally. See: https://docs.starrocks.io/docs/loading/SQL_transaction/ for more details.
- **Name length** — StarRocks allows table names up to 1024 characters, but database names are limited to 256. SQLMesh uses the stricter 256-character limit for all identifiers. See: https://docs.starrocks.io/docs/sql-reference/System_limit/ for more details.
- **No materialized views** — StarRocks has support for materialized views, but SQLMesh doesn't support them yet. Use regular views or tables.
