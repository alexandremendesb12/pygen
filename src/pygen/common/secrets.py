import os

def get_secret(key: str, scope: str = "genesis-secrets") -> str:
    """
    Securely retrieves a secret from Databricks Secret Scope (Azure Key Vault-backed),
    falling back to environment variable for local development.

    Args:
        key (str): Name of the secret key.
        scope (str): Name of the Databricks Secret Scope.

    Returns:
        str: Secret value.
    """
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        return os.getenv(key, "")
