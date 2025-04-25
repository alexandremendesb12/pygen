class PygenBaseException(Exception):
    """Exceção base para todos os erros da biblioteca Pygen."""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


# =========================
# 1. Erros de I/O e leitura
# =========================

class DataReadError(PygenBaseException):
    """Erro ao tentar ler um dataset com Spark."""
    pass

class DataWriteError(PygenBaseException):
    """Erro ao tentar escrever um dataset com Spark."""
    pass

class UnsupportedFormatError(PygenBaseException):
    """Erro para formatos de dados não suportados."""
    pass

class DataWriteError(PygenBaseException):
    """Erro ao tentar escrever um dataset com Spark."""
    pass


# ========================
# 2. Erros de pipeline/execução
# ========================

class InvalidQueryError(PygenBaseException):
    """Query SQL inválida passada ao Spark."""
    pass

class PipelineExecutionError(PygenBaseException):
    """Erro durante execução de pipeline principal."""
    pass


# ========================
# 3. Erros de configuração
# ========================

class ConfigurationLoadError(PygenBaseException):
    """Erro ao carregar configurações (YAML, ENV, etc.)."""
    pass

class MissingConfigurationError(PygenBaseException):
    """Campo obrigatório ausente na configuração."""
    pass


# ========================
# 4. MLflow / tracking
# ========================

class MlflowTrackingError(PygenBaseException):
    """Erro relacionado ao uso do MLflow Tracking."""
    pass

class ModelLoggingError(PygenBaseException):
    """Erro ao logar modelo ou artefato no MLflow."""
    pass
