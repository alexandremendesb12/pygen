import yaml
import os
from pyspark.sql import SparkSession

def get_env(default: str = "dev") -> str:
    """
    Retorna o ambiente atual com base na tag 'ENV' do cluster Databricks.

    Args:
        default (str): Valor padrão caso a tag não esteja configurada.

    Returns:
        str: Ambiente ativo (ex: 'dev', 'prod').
    """
    try:
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        env = spark.conf.get("spark.databricks.clusterUsageTags.ENV")
        return env.lower()
    except Exception:
        return default.lower()

def load_config(path: str) -> dict:
    """
    Loads a YAML configuration file as a dictionary.
    If the file does not exist or cannot be loaded, returns an empty dict.

    Args:
        path (str): Path to the YAML config file.

    Returns:
        dict: Loaded configuration, or empty dict if file not found or invalid.
    """
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        return {}

def get_config_value(key: str, default=None, config: dict = None):
    """
    Retorna um valor de configuração, com fallback para default.

    Args:
        key (str): Nome da chave no dicionário.
        default: Valor padrão caso a chave não exista.
        config (dict): Configurações carregadas via YAML.

    Returns:
        Any: Valor da configuração.
    """
    if config is None:
        return default
    return config.get(key, default)
