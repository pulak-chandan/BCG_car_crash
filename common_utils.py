from pyspark.sql import SparkSession

class commonUtils:
    def __init__(self):
        pass

    def read_csv(self, spark, path):
        """
        This function loads a csv file into a dataframe
        :param spark: spark session object
        :param path: path of the csv file to be read
        :return: returns a dataframe
        """
        try:
            df = spark.read.format("csv").option("header", True).load(path)
            return df
        except Exception as e:
            print("Error occurred while reading input file!")
            print(e)
            exit(0)

    def create_session(self):
        """
        This function creates and returns a spark session object
        :return: returns spark session object
        """
        try:
            spark = SparkSession.builder.appName("carCrashAnalysis").getOrCreate()
            return spark
        except Exception as e:
            print("Error occurred while creating Spark Session!")
            print(e)
            exit(0)

    def write_csv(self, df, path):
        """
        This function saves a dataframe as a CSV file
        :param df: dataframe to be written into a CSV file
        :param path: path of the output CSV file
        :return: None
        """
        df.coalesce(1).write.format("csv").option("header", True).save(path, mode='overwrite')

    def write_text_data(self, text, path):
        """
        This function writes test data into .txt file
        :param text: text data to be written into a .txt file
        :param path: path of the output file
        :return: None
        """
        try:
            op_file = open(path, "w+")
            op_file.write(text)
            op_file.close()
        except Exception as e:
            print("Error occurred while writing text data!")
            print(e)
            exit(0)
