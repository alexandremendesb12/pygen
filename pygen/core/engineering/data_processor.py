from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.errors import PySparkException
from pyspark.sql.utils import AnalysisException

from pygen.service.spark.spark_session import SparkSession
from pygen.service.log.auto_logger import AutoLogMixin
from pygen.core.common.exceptions import DataReadError, DataWriteError


class DataProcessor(AutoLogMixin):
    """
    Base class for feature engineering tasks, providing reusable data loading methods,
    Spark session handling, and automatic logging.
    """

    SUPPORTED_FORMATS = {"delta", "parquet"}

    def __init__(
        self,
        dependencies: Optional[List[Dict]] = None,
        spark: Optional[SparkSession] = None,
        product_name: Optional[str] = None,
    ):
        self.spark = spark or SparkSession(app_name=product_name or "DataProcessor")
        self.dependencies = dependencies or []
        super().__init__(name=product_name or "DataProcessor", level="DEBUG")

    def read_parquet(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        options = options or {}
        try:
            self.logger.debug(f"Lendo arquivo Parquet de {path} com opções: {options}")
            return self.spark.read.options(**options).parquet(path)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[Parquet] Falha ao ler {path}: {e}")
            raise DataReadError(f"Erro ao ler arquivo Parquet em {path}") from e

    def read_delta(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        options = options or {}
        try:
            self.logger.debug(f"Lendo arquivo Delta de {path} com opções: {options}")
            return self.spark.read.format("delta").options(**options).load(path)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[Delta] Falha ao ler {path}: {e}")
            raise DataReadError(f"Erro ao ler arquivo Delta em {path}") from e

    def read_csv(self, path: str, header: bool = True, sep: str = ",", options: Optional[Dict[str, str]] = None) -> DataFrame:
        options = options or {}
        try:
            self.logger.debug(f"Lendo arquivo CSV de {path} | sep={sep}, header={header}, opções: {options}")
            return self.spark.read.options(**options).csv(path, header=header, sep=sep)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[CSV] Falha ao ler {path}: {e}")
            raise DataReadError(f"Erro ao ler arquivo CSV em {path}") from e

    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", options: Optional[Dict[str, str]] = None) -> None:
        options = options or {}
        try:
            self.logger.debug(f"Escrevendo Parquet em {path}, modo={mode}, opções={options}")
            df.write.mode(mode).options(**options).parquet(path)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[Parquet] Falha ao escrever em {path}: {e}")
            raise DataWriteError(f"Erro ao escrever Parquet em {path}") from e

    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite", options: Optional[Dict[str, str]] = None) -> None:
        options = options or {}
        try:
            self.logger.debug(f"Escrevendo Delta em {path}, modo={mode}, opções={options}")
            df.write.format("delta").mode(mode).options(**options).save(path)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[Delta] Falha ao escrever em {path}: {e}")
            raise DataWriteError(f"Erro ao escrever Delta em {path}") from e

    def write_csv(self, df: DataFrame, path: str, mode: str = "overwrite", header: bool = True, sep: str = ",", options: Optional[Dict[str, str]] = None) -> None:
        options = options or {}
        try:
            self.logger.debug(f"Escrevendo CSV em {path}, header={header}, sep={sep}, modo={mode}, opções={options}")
            df.write.mode(mode).options(**options).csv(path, header=header, sep=sep)
        except (AnalysisException, PySparkException, Exception) as e:
            self.logger.error(f"[CSV] Falha ao escrever em {path}: {e}")
            raise DataWriteError(f"Erro ao escrever CSV em {path}") from e

    def run_query(self, query: str):
        if not isinstance(query, str):
            message = "Query parameter must be a string!"
            self.logger.error(message)
            raise ValueError(message)

        try:
            self.logger.debug(f"Executando query Spark SQL: {query}")
            return self.spark.sql(query)
        except Exception as e:
            self.logger.error(f"Erro ao executar query SQL: {e}")
            raise
