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

def load_data(file_path):

    '''
    Fungsi ini digunakan untuk melakukan proses Extract, yaitu membaca file
    CSV mentah dan mengubahnya menjadi Spark DataFrame.

    Parameters:
        file_path : Lokasi file CSV yang ingin dibaca.
    '''

    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data

if __name__ == "__main__":
    file_path = "/opt/airflow/data/P2M3_ahmad_darmawan_data_raw.csv" 
    df = load_data(file_path)

    # save to csv
    df.write.csv("/opt/airflow/data/data_extract_output", header=True, mode="overwrite")