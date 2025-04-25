import sys
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pygen.service.spark.spark_session import SparkSession

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    (1, "Alice", 29, "New York"),
    (2, "Bob", 35, "São Paulo"),
    (3, "Charlie", 42, "Berlin"),
    (4, "Diana", 31, "London")
]

class MyClass():
    def __init__(self):
        """
        Initializes the FeatureEngineering instance.

        This calls the superclass constructor and sets the SparkSession instance
        with the name "MyJob" and a tag "owner" with value "ds-team".
        """
        super().__init__()
        self.spark_session = SparkSession(app_name="MyJob", tags={"owner": "ds-team"})
        
    def load_data(self): 
        """
        Loads data into a DataFrame using the predefined schema.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing the loaded data.
        """
        df = self.spark_session.createDataFrame(data, schema)
        return df

if __name__ == "__main__":
    my_class = MyClass()
    df = my_class.load_data()
    df.show()