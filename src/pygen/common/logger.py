import logging
import sys
from config import get_env


class GenesisLogger:
    """
    GenesisLogger fornece um logger padronizado e contextualizado
    para todos os módulos da plataforma Genesis.
    """

    def __init__(self, name: str = "Genesis", level: str = None):
        """
        Inicializa o logger com nome, nível e formatação personalizada.

        Args:
            name (str): Nome do logger.
            level (str): Nível de logging (DEBUG, INFO, WARNING, ERROR).
        """
        self.name = name
        self.level = level.upper() if level else "INFO"
        self.env = get_env()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        Cria e configura a instância do logger.

        Returns:
            logging.Logger: Logger configurado.
        """
        logger = logging.getLogger(self.name)

        if not logger.handlers:
            logger.setLevel(getattr(logging, self.level, logging.INFO))

            formatter = logging.Formatter(
                fmt=f"[%(asctime)s] [%(levelname)s] [{self.name}] [env={self.env}] - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)

            logger.addHandler(handler)

        return logger

    def get(self) -> logging.Logger:
        """
        Retorna o logger configurado.

        Returns:
            logging.Logger: Logger instanciado.
        """
        return self.logger
