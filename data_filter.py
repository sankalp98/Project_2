import argparse

from pyspark.sql import SparkSession

def filter_data(data_source, output_uri):
    """
    :param data_source: The URI of your log data CSV, such as 's3://DOC-EXAMPLE-BUCKET/food-establishment-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/restaurant_violation_results'.
    """
    with SparkSession.builder.appName("Total log erros").getOrCreate() as spark:
        # Load the logs CSV data
        if data_source is not None:
            logs_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        logs_df.createOrReplaceTempView("logs_data")

        # Create a DataFrame of the logs with error code in range of 400-500
        logs_error_400 = spark.sql("""SELECT ip,time_stamp,request_type,error_code,port 
          FROM logs_data 
          ORDER BY error_code ASC""")

        # Write the results to the specified output URI
        logs_error_400.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="s3://datalogsss/Logs")
    parser.add_argument(
        '--output_uri', help="s3://datalogsss/")
    args = parser.parse_args()

    filter_data(args.data_source, args.output_uri)
			