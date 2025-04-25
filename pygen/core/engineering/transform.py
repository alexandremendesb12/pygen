import os
from typing import Dict, List, Optional
from pyspark.sql.utils import AnalysisException
from pygen.service.spark.spark_session import SparkSession
from pygen.service.log.logger import AutoLogMixin

class TransformData(AutoLogMixin):
    """
    Base class for feature engineering tasks, providing reusable data loading methods,
    Spark session handling, and automatic logging.
    """

    SUPPORTED_FORMATS = {"delta", "parquet"}

    def __init__(
        self,
        dependencies: Optional[List[Dict]] = None,
        spark_session: Optional[SparkSession] = None,
        product_name: Optional[str] = None,
    ):
        self.spark_session = spark_session or SparkSession(app_name=product_name or "TransformData")
        self.dependencies = dependencies or []
        super().__init__(name=product_name or "TransformData", level="DEBUG")

    def _read_data(self, path: str, format: str = "delta"):
        """
        Internal method to read a dataset (Delta, Parquet, etc.).

        Raises:
            ValueError: If format is unsupported.
            RuntimeError: If data cannot be read.

        Returns:
            DataFrame: Loaded Spark DataFrame.
        """
        if format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format '{format}'. Supported formats: {self.SUPPORTED_FORMATS}")

        try:
            df = self.spark_session.read.format(format).load(path)
            return df
        except AnalysisException as ae:
            raise RuntimeError(f"Spark could not read data at '{path}': {ae}")
        except Exception as e:
            raise RuntimeError(f"Error reading data from '{path}' with format '{format}': {e}")

    def load_table(self, table: str, layer: Optional[str] = None, format: str = "delta"):
        """
        Loads a dataset from a managed data layer path with support for Delta and Parquet.

        Args:
            table (str): Table name or relative path.
            layer (str, optional): Optional layer name (e.g., 'bronze', 'silver').
            format (str): Format to read (default is 'delta').

        Raises:
            RuntimeError: If the dataset cannot be loaded.

        Returns:
            DataFrame: Loaded Spark DataFrame.
        """
        full_path = f"mnt/{layer}/{table}" if layer else table

        try:
            return self._read_data(full_path, format=format)
        except Exception as e:
            raise RuntimeError(f"Failed to load dataset from '{full_path}': {e}")
    
    def run_query(self, query: str):
        """
        Executes a SQL query on the current Spark session.

        Args:
            query (str): SQL query to execute.
        """
        if not isinstance(query, str):
            message = "Query parameter must be a string!"
            return self.logger.error(message)
        
        return self.spark_session.sql(query)
