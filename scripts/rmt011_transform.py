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

def transform(data):
    """
    Melakukan proses transformasi data seperti:
    - Menghapus kolom yang tidak digunakan.
    - Mengubah tipe data kolom tertentu.

    Parameters:
        data (DataFrame): Data hasil extract yang akan ditransformasi.

    Returns:
        DataFrame : Data yang sudah dibersihkan dan siap di-load.
    """

    # 1. Drop kolom yang tidak dipakai
    cols_to_drop = [
        "Hire_Date",
        "Team_Size",
        "Training_Hours",
        "Promotions",
        "Sick_Days",
        "Overtime_Hours",
        "Remote_Work_Frequency"
    ]
    data = data.drop(*cols_to_drop)
    data = data.withColumn("Resigned", data["Resigned"].cast("integer"))
    return data

# Langsung eksekusi transformasi
spark = SparkSession.builder.appName("Transform").getOrCreate()

# Baca hasil extract
df = spark.read.csv(
    "/opt/airflow/data/data_extract_output",
    header=True,
    inferSchema=True
)

# Transform
transformed_df = transform(df)

# Simpan hasil transform ke CSV 
transformed_df.write.mode("overwrite").option("header", True).csv("/opt/airflow/data/data_transform_output")