import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pygen.core.engineering.feature_engineering import FeatureEngineering

class MyFeatureEngineering(FeatureEngineering):
    def __init__(self):
        """
        Initializes the FeatureEngineering instance.

        This calls the superclass constructor and sets the SparkSession instance
        with the name "MyJob" and a tag "owner" with value "ds-team".
        """
        super().__init__()

    def load_data(self):
        """
        Loads a dataset from a managed data layer path with support for Delta and Parquet.

        Args:
            table (str): Table name or relative path.
            layer (str, optional): Optional layer name (e.g., 'bronze', 'silver').
            format (str): Format to read (default is 'delta').

        Raises:
            RuntimeError: If the dataset cannot be loaded.

        Returns:
            DataFrame: Loaded Spark DataFrame.
        """
        df = self.load_table('Customers/PotentialCustomers', layer='bronze', format='delta')
        return df

if __name__ == "__main__":
    my_fe = MyFeatureEngineering()
    df = my_fe.load_data()
    df.show()