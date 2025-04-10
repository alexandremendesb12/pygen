import os
from pyspark.sql import SparkSession

class DataLakeConnector:
    """
    Handles secure connection configuration for Azure Data Lake Storage Gen2
    based on the execution environment and secret management strategy.

    Supports two modes:
    - Environment variables (local or containerized environments)
    - Databricks secrets (recommended for production)

    Attributes:
        spark (SparkSession): Active Spark session.
        env (str): Environment name ("dev" or "prod").
        use_databricks_secrets (bool): Flag indicating whether to use Databricks secrets.
        container (str): Azure Storage container name.
        account_name (str): Azure Storage account name, inferred by environment.
        base_path (str): Base ABFSS path to the container.
    """

    def __init__(self, spark: SparkSession, env: str, use_databricks_secrets: bool = False):
        """
        Initializes the connector with environment settings and configures Spark.

        Args:
            spark (SparkSession): Active Spark session.
            env (str): Target environment ("dev" or "prod").
            use_databricks_secrets (bool): Whether to load secrets from Databricks.
        """
        self.spark = spark
        self.env = env.lower()
        self.use_databricks_secrets = use_databricks_secrets
        self.container = "genesis"
        self.account_name = self._get_account_name()
        self._set_spark_configs()
        self.base_path = self._build_base_path()

    def _get_account_name(self) -> str:
        """
        Returns the Azure Storage account name based on the current environment.

        Returns:
            str: Storage account name.
        """
        if self.env == "dev":
            return "genesisdatadev"
        elif self.env == "prod":
            return "genesisdataprod"
        else:
            raise ValueError(f"Unrecognized environment: {self.env}")

    def _get_storage_key(self) -> str:
        """
        Retrieves the storage account key from environment variables or Databricks secrets.

        Returns:
            str: The Azure Storage access key.

        Raises:
            ValueError: If the environment is not mapped to a secret key.
            EnvironmentError: If the required secret is not found.
        """
        if self.use_databricks_secrets:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)

            scope = "genesis-secrets"
            key_map = {
                "dev": "storage-key-dev",
                "prod": "storage-key-prod"
            }
            secret_key = key_map.get(self.env)
            if not secret_key:
                raise ValueError(f"No secret key mapping for environment: {self.env}")
            return dbutils.secrets.get(scope=scope, key=secret_key)

        else:
            env_var_map = {
                "dev": "AZURE_STORAGE_KEY_DEV",
                "prod": "AZURE_STORAGE_KEY_PROD"
            }
            key_var = env_var_map.get(self.env)
            if not key_var:
                raise ValueError(f"No environment variable mapping for environment: {self.env}")

            storage_key = os.getenv(key_var)
            if not storage_key:
                raise EnvironmentError(f"Missing required environment variable: {key_var}")
            return storage_key

    def _set_spark_configs(self):
        """
        Sets the Spark configuration required to access the Azure Data Lake.
        """
        storage_key = self._get_storage_key()
        config_key = f"fs.azure.account.key.{self.account_name}.dfs.core.windows.net"
        self.spark.conf.set(config_key, storage_key)

    def _build_base_path(self) -> str:
        """
        Constructs the base ABFSS path to the container.

        Returns:
            str: The base path.
        """
        return f"abfss://{self.container}@{self.account_name}.dfs.core.windows.net"

    def get_path(self, subpath: str = "") -> str:
        """
        Builds the full path to a specific subdirectory inside the container.

        Args:
            subpath (str): Relative path within the container.

        Returns:
            str: Full ABFSS path.
        """
        return f"{self.base_path}/{subpath}".rstrip("/")
