from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict

class DataReader:
    """
    Serviço de leitura de datasets com Spark.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_parquet(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        """
        Lê um arquivo Parquet com a biblioteca Spark.

        Args:
            path: Caminho do arquivo a ser lido.
            options: Dicionário de opções adicionais para a leitura do arquivo.

        Returns:
            DataFrame: DataFrame lido do arquivo Parquet.
        """

        options = options or {}
        return self.spark.read.options(**options).parquet(path)

    def read_delta(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        """
        Lê um arquivo Delta Lake com a biblioteca Spark.

        Args:
            path: Caminho do arquivo a ser lido.
            options: Dicionário de op es adicionais para a leitura do arquivo.

        Returns:
            DataFrame: DataFrame lido do arquivo Delta.
        """
        options = options or {}
        return self.spark.read.format("delta").options(**options).load(path)

    def read_csv(self, path: str, header: bool = True, sep: str = ",", options: Optional[Dict[str, str]] = None) -> DataFrame:
        """
        Lê um arquivo CSV com a biblioteca Spark.

        Args:
            path: Caminho do arquivo a ser lido.
            header: Se o arquivo CSV tem um cabeçalho. Padrão: True.
            sep: Caractere separador de colunas. Padrão: ",".
            options: Dicionário de op es adicionais para a leitura do arquivo.

        Returns:
            DataFrame: DataFrame lido do arquivo CSV.
        """
        options = options or {}
        return self.spark.read.options(**options).csv(path, header=header, sep=sep)

    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", options: Optional[Dict[str, str]] = None) -> None:
        """
        Salva um DataFrame em um arquivo Parquet.

        Args:
            df: DataFrame a ser salvo.
            path: Caminho do arquivo a ser salvo.
            mode: Modo de escrita. Pode ser "overwrite", "append" ou "error".
            options: Dicionário de op es adicionais para a escrita do arquivo.
        """
        options = options or {}
        df.write.mode(mode).options(**options).parquet(path)

    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite", options: Optional[Dict[str, str]] = None) -> None:
        """
        Salva um DataFrame em um arquivo Delta Lake.

        Args:
            df: DataFrame a ser salvo.
            path: Caminho do arquivo a ser salvo.
            mode: Modo de escrita. Pode ser "overwrite", "append" ou "error".
            options: Dicion rio de op es adicionais para a escrita do arquivo.
        """
        options = options or {}
        df.write.format("delta").mode(mode).options(**options).save(path)

    def write_csv(self, df: DataFrame, path: str, mode: str = "overwrite", header: bool = True, sep: str = ",", options: Optional[Dict[str, str]] = None) -> None:
        """
        Salva um DataFrame em um arquivo CSV.

        Args:
            df: DataFrame a ser salvo.
            path: Caminho do arquivo a ser salvo.
            mode: Modo de escrita. Pode ser "overwrite", "append" ou "error".
            header: Se o arquivo CSV deve ter um cabe alho. Padr o: True.
            sep: Caractere separador de colunas. Padr o: ",".
            options: Dicion rio de op es adicionais para a escrita do arquivo.
        """
        options = options or {}
        df.write.mode(mode).options(**options).csv(path, header=header, sep=sep)
