from pyspark.sql import SparkSession

def create_spark(app_name: str = "MOTH-Pipeline", master: str = "local[*]", timezone: str = "Australia/Melbourne"):
    """Create and return a SparkSession with sensible defaults.
    Use local[*] to use all logical cores on the machine. Set the session timezone as required.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")          
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
