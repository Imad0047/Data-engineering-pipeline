import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType , IntegerType , BooleanType , DoubleType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id TEXT PRIMARY KEY,
            name TEXT,
            latitude double,
            longitude double,
            num_bikes int,
            num_empty_docks int,
            num_docks int,
            num_standard_bikes int,
            num_ebikes int,
            installed boolean,
            locked boolean,
            temporary boolean
        );
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("inserting data...")

    id = kwargs.get('id')
    name = kwargs.get('name')
    latitude = kwargs.get('latitude')
    longitude = kwargs.get('longitude')
    num_bikes = kwargs.get('num_bikes')
    nb_empty_docks = kwargs.get('num_empty_docks')
    nb_docks = kwargs.get('num_docks')
    nb_standard_bikes = kwargs.get('num_standard_bikes')
    nb_ebikes = kwargs.get('num_ebikes')
    installed = kwargs.get('installed')
    locked = kwargs.get('locked')
    temporary = kwargs.get('temporary')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(
                id, name, latitude, longitude, num_bikes,
                nb_empty_docks, nb_docks, nb_standard_bikes,
                nb_ebikes, installed, locked, temporary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, name, latitude, longitude, num_bikes,
              nb_empty_docks, nb_docks, nb_standard_bikes,
              nb_ebikes, installed, locked, temporary))
        logging.info(f"Data inserted for {id} {name}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'bikepoint_stream') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df



def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        cas_session = cluster.connect()
        logging.info("Cassandra connection established successfully!")
        return cas_session
    except Exception as e:
        import traceback
        logging.error("Could not create cassandra connection: %s", traceback.format_exc())
        return None



def create_selection_df_from_kafka(spark_df):
    schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("num_bikes", IntegerType(), True),
    StructField("num_empty_docks", IntegerType(), True),
    StructField("num_docks", IntegerType(), True),
    StructField("num_standard_bikes", IntegerType(), True),
    StructField("num_ebikes", IntegerType(), True),
    StructField("installed", BooleanType(), True),
    StructField("locked", BooleanType(), True),
    StructField("temporary", BooleanType(), True)
            ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            #  AFFICHER CE QUE SPARK LIT DE KAFKA
            query_console = (selection_df.writeStream
                 .format("console")
                 .option("truncate", False)
                 .outputMode("append")
                 .start())
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint3')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
