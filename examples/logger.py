import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pygen.service.log.logger import AutoLogMixin

class FeatureEngineering(AutoLogMixin):
    def __init__(self):
        """
        Initializes the FeatureEngineering instance.

        This calls the superclass constructor with the name "FeatureEngineering" and
        sets the log level to "DEBUG".
        """
        super().__init__(name="FeatureEngineering", level="DEBUG")
    
    def sum_values(self, first_value: int, second_value: int) -> int:
        return first_value + second_value
    
if __name__ == "__main__":
    fe = FeatureEngineering()
    result = fe.sum_values(4,5)


