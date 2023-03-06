"""

SOUMIL SHAH

"""

try:
    import sys
    from datetime import date
    import ast
    import datetime
    from ast import literal_eval
    import re
    import boto3
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    import os
    import json
    from dataclasses import dataclass
    from pyspark.sql.functions import from_json, col
    import pyspark.sql.functions as F
except Exception as e:
    pass


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3",
                                   aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                                   aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                                   region_name=os.getenv("AWS_REGION"))

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


@dataclass
class HUDISettings:
    """Class for keeping track of an item in inventory."""

    table_name: str
    path: str


class HUDIIncrementalReader(AWSS3):
    def __init__(self, bucket, hudi_settings, spark_session):
        AWSS3.__init__(self, bucket=bucket)
        if type(hudi_settings).__name__ != "HUDISettings": raise Exception("please pass correct settings ")
        self.hudi_settings = hudi_settings
        self.spark = spark_session

    def __check_meta_data_file(self):
        """
        check if metadata for table exists
        :return: Bool
        """
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        return self.item_exists(Key=file_name)

    def __read_meta_data(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"

        return ast.literal_eval(self.get_item(Key=file_name).decode("utf-8"))

    def __push_meta_data(self, json_data):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.put_files(
            Key=file_name, Response=json.dumps(json_data)
        )

    def __get_begin_commit(self):
        self.spark.read.format("hudi").load(self.hudi_settings.path).createOrReplaceTempView("hudi_snapshot")
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_snapshot order by commitTime asc").limit(
            50).collect()))

        """begin from start """
        begin_time = int(commits[0]) - 1
        return begin_time

    def __read_inc_data(self, commit_time):
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.query.incremental.format': 'cdc',
            'hoodie.datasource.read.begin.instanttime': commit_time,
        }

        print("***")

        incremental_df = self.spark.read.format("hudi").options(**incremental_read_options).load(
            self.hudi_settings.path).createOrReplaceTempView("hudi_incremental")

        df = self.spark.sql("select * from  hudi_incremental")

        return df

    def __get_last_commit(self):

        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(ts_ms) from  hudi_incremental order by ts_ms asc").limit(
            50).collect()))

        last_commit = commits[len(commits) - 1]

        return last_commit

    def reset_bookmark(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.delete_object(Key=file_name)

    def __run(self):
        """Check the metadata file"""
        flag = self.__check_meta_data_file()

        """if metadata files exists load the last commit and start inc loading from that commit """
        if flag:
            meta_data = json.loads(self.__read_meta_data())
            print(f"""
            ******************LOGS******************
            meta_data {meta_data}
            last_processed_commit : {meta_data.get("last_processed_commit")}
            ***************************************
            """)

            read_commit = str(meta_data.get("last_processed_commit"))
            df = self.__read_inc_data(commit_time=read_commit)

            """if there is no INC data then it return Empty DF """
            if not df.rdd.isEmpty():
                last_commit = self.__get_last_commit()
                self.__push_meta_data(json_data=json.dumps({
                    "last_processed_commit": last_commit,
                    "table_name": self.hudi_settings.table_name,
                    "path": self.hudi_settings.path,
                    "inserted_time": datetime.datetime.now().__str__(),

                }))
                return df
            else:
                return df

        else:

            """Metadata files does not exists meaning we need to create  metadata file on S3 and start reading from begining commit"""

            read_commit = self.__get_begin_commit()

            df = self.__read_inc_data(commit_time=read_commit)
            last_commit = self.__get_last_commit()

            self.__push_meta_data(json_data=json.dumps({
                "last_processed_commit": last_commit,
                "table_name": self.hudi_settings.table_name,
                "path": self.hudi_settings.path,
                "inserted_time": datetime.datetime.now().__str__(),

            }))

            return df

    def read(self):
        """
        reads INC data and return Spark Df
        :return:
        """

        return self.__run()


def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc + '.' + c).alias(c)
                                for nc in nested_cols
                                for c in nested_df.select(nc + '.*').columns])
    return flat_df


def main():
    from dotenv import load_dotenv
    load_dotenv(".env")

    bucket = 'hudi-demos-emr-serverless-project-soumil'
    ELK_VERSION = "8.4.2"
    HUDI_VERSION = "0.13.0"
    SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:{HUDI_VERSION},org.elasticsearch:elasticsearch-spark-30_2.12:{ELK_VERSION} pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['AWS_ACCESS_KEY'] = "XXXXXXXXXXXX"
    os.environ['AWS_SECRET_KEY'] = 'XXXXXXXXXXXXXXXX'
    os.environ['AWS_REGION'] = 'us-east-1'

    db_name = "hudidb"
    table_name = "hudi_cdc_table"
    path = f"file:///C:/tmp/{db_name}/{table_name}"

    spark = SparkSession.builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config('className', 'org.apache.hudi') \
        .config('spark.sql.hive.convertMetastoreParquet', 'false') \
        .getOrCreate()

    helper = HUDIIncrementalReader(bucket=bucket,
                                   hudi_settings=HUDISettings(table_name='hudi_inc_table', path=path),
                                   spark_session=spark
                                   )
    # helper.reset_bookmark()
    df = helper.read()

    if not df.rdd.isEmpty():

        """Post Processing"""
        json_schema = spark.read.json(df.rdd.map(lambda row: row.after)).schema
        df = df.withColumn('json', from_json(col('after'), json_schema))
        df = flatten_df(df)

        columns_drop = ["before", "after"]
        for column in df.columns:
            if "hoodie" in column: columns_drop.append(column)

        df = df.drop(*columns_drop)
        print(df.show())

        # =========== Elastic Search Settings ===========================
        es_nodes = "localhost"
        es_port = "9200"
        # <TABLENAME>_<YEAR>_<MONTH>_<DAY>
        es_index = f"{table_name}-{date.today().year}_{date.today().month}_{date.today().day}"
        es_username = "XXXXXXXXXXXX"
        es_password = "XXXXXXXXXX"
        # ==============================================================

        df.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", es_nodes) \
            .option("es.port", es_port) \
            .option("es.mapping.id", "uuid") \
            .option("es.net.http.auth.user", es_username) \
            .option("es.net.http.auth.pass", es_password) \
            .mode("append") \
            .save("{}".format(es_index))


main()
