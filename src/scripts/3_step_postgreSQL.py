from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_IN = 'student.topic.cohort8.anisimovp_in'
TOPIC_OUT = 'cohort8.anisimovp_out'

spark_master = 'local'
spark_app_name = "RestaurantSubscribeStreamingService"

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required '
                              'username=\"de-student\" password=\"ltcneltyn\";',
}
kafka_bootstrap_servers = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .master(spark_master) \
    .appName(spark_app_name) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://localhost:5432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'jovyan') \
                    .option('password', 'jovyan') \
                    .load()

# запускаем стриминг
if __name__ == "__main__":
    result = subscribers_restaurant_df

    result.show()

