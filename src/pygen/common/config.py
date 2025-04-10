import yaml
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
    Carrega um arquivo de configuração YAML como dicionário.

    Args:
        path (str): Caminho para o arquivo YAML.

    Returns:
        dict: Configurações carregadas.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)

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
