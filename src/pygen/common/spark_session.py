import os
import logging
from pyspark.sql import SparkSession as PySparkSession
from secrets import get_secret
from config import get_env


class SparkSession:
    """
    Genesis-standard SparkSession wrapper.
    
    This class automatically initializes a SparkSession with predefined configurations,
    supporting Delta Lake, Azure authentication, environment-based setup, external YAML configs,
    logging, and custom tags for traceability.

    Example:
        self.spark_session = SparkSession()
        df = self.spark_session.read.parquet("/path/to/data")

    Args:
        app_name (str, optional): Name of the Spark application. Defaults to "GenesisApp-<env>".
        config_path (str, optional): Path to a YAML config file with Spark options.
        tags (dict, optional): Tags added to the session as metadata for observability.
        shuffle_partitions (str, optional): Default value for shuffle partitions. Defaults to "200".
    """

    def __init__(
        self,
        app_name: str = None,
        config_path: str = None,
        tags: dict = None,
        shuffle_partitions: str = None
    ):
        """
        Initializes the Genesis SparkSession and validates Delta support.
        """
        self.env = get_env()
        self.app_name = app_name or f"GenesisApp-{self.env}"
        self.config_path = config_path or "configs/spark_defaults.yaml"
        self.shuffle_partitions = shuffle_partitions or "200"
        self.tags = tags or {}
        self.logger = self._setup_logger()
        self.extra_configs = self._load_external_configs()
        self.session = self._create_spark_session()
        self._validate_delta_support()
        self._log_session_info()

    def _setup_logger(self):
        """
        Sets up a logger specific to the SparkSession context.

        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger("SparkSession")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _load_external_configs(self) -> dict:
        """
        Loads additional Spark configuration from a YAML file.

        Returns:
            dict: Dictionary of Spark configurations.
        """
        try:
            cfg = load_config(self.config_path)
            self.logger.info(f"Configuration loaded from {self.config_path}")
            return cfg.get("spark", {})
        except Exception as e:
            self.logger.warning(f"Could not load external configs: {e}")
            return {}

    def _create_spark_session(self) -> PySparkSession:
        """
        Builds and returns a SparkSession using base, custom and external configs.

        Returns:
            pyspark.sql.SparkSession: Configured Spark session.
        """
        builder = PySparkSession.builder.appName(self.app_name)

        base_config = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.shuffle.partitions": self.shuffle_partitions,
            "spark.sql.session.timeZone": "America/Sao_Paulo",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": get_secret("AZURE_CLIENT_ID"),
            "fs.azure.account.oauth2.client.secret": get_secret("AZURE_CLIENT_SECRET"),
            "fs.azure.account.oauth2.client.endpoint": get_secret("AZURE_OAUTH_ENDPOINT"),
        }

        # Add custom tags as Spark properties
        for key, value in self.tags.items():
            base_config[f"spark.genesis.tag.{key}"] = value

        all_configs = {**base_config, **self.extra_configs}
        for key, value in all_configs.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def _validate_delta_support(self):
        """
        Validates whether Delta Lake is enabled in the current SparkSession.

        Raises:
            RuntimeError: If Delta Lake is not properly configured.
        """
        try:
            self.session.sql("CREATE OR REPLACE TEMP VIEW __delta_test AS SELECT 1")
            self.session.sql("CREATE OR REPLACE TABLE __test_delta (id INT) USING DELTA")
            self.session.sql("DROP TABLE __test_delta")
            self.logger.info("Delta support validation: ✅ OK")
        except Exception as e:
            self.logger.error("❌ Delta Lake not configured correctly.")
            raise RuntimeError("Delta Lake not enabled") from e

    def _log_session_info(self):
        """
        Logs essential SparkSession details: app name, environment, version, and tags.
        """
        self.logger.info(f"SparkSession initialized - App: {self.app_name}, Env: {self.env}")
        self.logger.info(f"Spark version: {self.session.version}")
        if self.tags:
            self.logger.info(f"Applied tags: {self.tags}")

    def __getattr__(self, name):
        """
        Delegates attribute access to the underlying SparkSession instance.

        Args:
            name (str): The method or property being accessed.

        Returns:
            Any: Corresponding attribute or method from SparkSession.
        """
        return getattr(self.session, name)
