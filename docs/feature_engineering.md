
# 🧠 FeatureEngineering Base Class

`FeatureEngineering` is a foundational class for managing data loading, transformation logic, and reuse across feature pipelines. It extends `AutoLogMixin` for automatic logging and includes a ready-to-use SparkSession.

## 🔧 Features

- Auto-managed SparkSession
- Safe and validated dataset loading
- Format support: Delta, Parquet
- Layered path abstraction (e.g., bronze/silver/gold)
- Logging of entry/exit and exceptions via `AutoLogMixin`

## 🧱 Initialization

```python
from pygen.core.engineering.feature_engineering import FeatureEngineering

query = """
    SELECT 
        id,
        name,
        age
    FROM 
        customers.accounts.profile
""" 

class MyFeature(FeatureEngineering):
    def __init__():
        super().__init__()
    
    def process():
        df = self.load_table("customers", layer="silver")
        query_df = self.run_query(query)
        
        return df.join(query, on="id", how="inner")
```

## 🧩 Methods

### `load_table(table: str, layer: Optional[str] = None, format: str = "delta")`
Loads a dataset by combining the layer and table path, and validates supported formats.

### `run_query(query: str)`
Loads data using SQL query and return a spark dataframe.

### `_read_data(path: str, format: str = "delta")`
Internal method for direct path-based reading with validation and error handling.

## ⚠️ Error Handling

- Raises `ValueError` for unsupported formats.
- Raises `RuntimeError` with descriptive message if dataset can't be read.

---
