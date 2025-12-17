'''
=================================================
Milestone 3

Nama  : Ahmad Darmawan
Batch : CODA-011-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL
ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai produktifitas karyawan.
=================================================
'''

from pyspark.sql import SparkSession
from pymongo import MongoClient

def spark_to_mongo(df, mongo_uri, db_name, collection_name):
    """
    Mengirim data dari DataFrame PySpark ke MongoDB.

    Parameters:
        df (DataFrame):
            DataFrame hasil transformasi yang akan dimasukkan ke MongoDB.

    Proses yang dilakukan:
        1. Convert DataFrame PySpark menjadi list of dict.
        2. Membuat koneksi ke MongoDB.
        3. Mengubah data menjadi BSON
    """

    # Konversi Pyspark DF -> Python dict
    data_list = [row.asDict() for row in df.collect()]

    # Mongo connection
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert multiple documents
    if data_list:
        collection.insert_many(data_list)
        print(f"{len(data_list)} documents inserted.")
    else:
        print("No data to insert.")

# Langsung eksekusi load ke MongoDB
spark = SparkSession.builder.appName("LoadToMongo").getOrCreate()

# Baca hasil transform dari Spark
df = spark.read.csv(
    "/opt/airflow/data/data_transform_output",
    header=True,
    inferSchema=True
)

# Masukkan ke MongoDB
spark_to_mongo(
    df,
    mongo_uri="mongodb+srv://aan:aan@cluster0.gbtvsqf.mongodb.net/",
    db_name="tugasm3",
    collection_name="employee"
)