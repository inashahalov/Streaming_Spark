from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# ================================
# Создание SparkSession
# ================================
spark = SparkSession.builder \
    .appName("SalesStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================
# Определение схемы
# ================================
schema = StructType() \
    .add("store", StringType()) \
    .add("item", StringType()) \
    .add("price", DoubleType()) \
    .add("ts", IntegerType())

# ================================
# Чтение из Kafka
# ================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .load()

# ================================
# Парсинг и преобразование времени
# ================================
parsed = df.select(
    col("key").cast("string").alias("store"),
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ingestion_time")
).select(
    "store",
    "data.item",
    "data.price",
    "data.ts",
    "kafka_ingestion_time"
)

# Преобразуем Unix-время в timestamp
with_event_time = parsed \
    .withColumn("event_time", col("ts").cast("timestamp"))

# Добавляем watermark
with_watermark = with_event_time \
    .withWatermark("event_time", "30 seconds")

# Агрегация по 30-секундным окнам
windowed_sales = with_watermark \
    .groupBy("store", window(col("event_time"), "30 seconds")) \
    .agg(
        sum("price").alias("total_revenue"),
        sum("price").cast("int").alias("total_revenue_int"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end")
    ) \
    .select(
        "store",
        "total_revenue_int",
        "window_start",
        "window_end"
    )

# ================================
# Запуск потока
# ================================
query = (
    windowed_sales.writeStream
    .outputMode("update")  # обновляем только изменённые окна
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime='10 seconds')  # триггер каждые 10 сек
    .start()
)

print("✅ Spark начал слушать топик 'sales'...")
print("📊 Агрегация по 30-секундным окнам. Ожидание данных...")

# ================================
# Удержание приложения
# ================================
query.awaitTermination()