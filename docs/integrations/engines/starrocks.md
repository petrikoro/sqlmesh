# StarRocks

[StarRocks](https://www.starrocks.io/) is a modern analytical database product based on an MPP architecture. It provides real-time analytical capabilities, supporting both high-concurrency point queries and high-throughput complex analysis.

SQLMesh supports StarRocks through its MySQL-compatible protocol, while providing StarRocks-specific optimizations for table models, indexing, partitioning, and other features.

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
| `concurrent_tasks` | The maximum number of concurrent tasks (default: 4)              | int    |    N     |

## Model Configuration

### Insert Overwrite Behavior

StarRocks supports native `INSERT OVERWRITE` syntax for partitioned tables. By default, SQLMesh uses the `DELETE_INSERT` strategy for broader compatibility. However, if your StarRocks cluster has `dynamic_overwrite` enabled, SQLMesh will automatically detect this and use the more efficient `INSERT OVERWRITE` strategy.

You can check if `dynamic_overwrite` is enabled in your cluster by running:

```sql
SELECT @@dynamic_overwrite;
```

For more information about INSERT OVERWRITE in StarRocks, see the [StarRocks documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#dynamic-overwrite).

## Limitations

- **Transactions**: StarRocks does not support traditional database transactions. SQLMesh handles this automatically.
- **Materialized Views**: While StarRocks supports asynchronous materialized views since v2.4, SQLMesh does not currently support them. Use regular views or tables instead.
- **Schema/Database Naming**: In StarRocks, schemas are equivalent to databases. When SQLMesh creates a schema, it creates a StarRocks database.
- **Identifier Length**: Database (schema) names are limited to 256 characters, while table names can be up to 1024 characters. SQLMesh uses the more restrictive 256 character limit for compatibility.
