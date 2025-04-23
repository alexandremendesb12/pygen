
# 🚀 PyGen SparkSession

The `SparkSession` class is a standardized and portable wrapper around `pyspark.sql.SparkSession`,
designed to simplify and unify the Spark initialization process across local, cloud, and Databricks environments.

## ✨ Features
- Loads external YAML configs
- Custom tags for observability (e.g., lineage, team, env)
- Predefined defaults for shuffle partitions and timezone
- Compatible with Delta Lake, Parquet, and more
- Logging with contextual info

## 🧱 Initialization

```python
from pygen.infra.service.spark.spark_session import SparkSession

spark = SparkSession(
    app_name="MyApp",
    tags={"env": "dev", "owner": "ds-team"}
)
df = spark.read.parquet("path/to/file.parquet")
```

## ⚙️ Configuration

- If `configs/spark_defaults.yaml` exists, it's automatically loaded.
- You can override configs using the `tags`, `app_name`, or YAML file.
- Tags are added as `spark.genesis.tag.{key}` Spark properties.

## 📤 Output Example (logs)
```
[INFO] SparkSession initialized - App: MyApp
[INFO] Spark version: 3.x.x
[INFO] Applied tags: {'env': 'dev', 'owner': 'ds-team'}
```

## 🧩 Methods

### `__getattr__`
Delegates method access to the underlying SparkSession instance.

### `_create_spark_session`
Builds a configured Spark session.

### `_load_external_configs`
Safely loads Spark configs from a YAML file.

---
