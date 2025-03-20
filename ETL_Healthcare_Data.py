from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode_outer, current_timestamp, lit, when, from_json, trim,
    regexp_extract, regexp_replace, split, initcap, concat_ws, expr, max, first, concat, coalesce, to_timestamp, date_format
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Cria a sessão Spark com configurações otimizadas
spark = SparkSession.builder \
    .appName("ETL para ClickHouse via JDBC") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.4.6-shaded.jar") \
    .getOrCreate()


# Caminho para os arquivos JSON
DATA_PATH = "data/*.json"

# Leitura dos dados e cache para performance
df = spark.read.option("multiline", "true").json(DATA_PATH).cache()
df = df.withColumn("entry_exploded", explode_outer(col("entry"))) \
       .select("entry_exploded.resource.*")

# Esquema para o campo "name"
NAME_SCHEMA = ArrayType(StructType([
    StructField("use", StringType(), True),
    StructField("family", StringType(), True),
    StructField("given", ArrayType(StringType()), True),
    StructField("prefix", ArrayType(StringType()), True)
]))

def process_patients(df):
    df_patients_raw = df.filter(col("resourceType") == "Patient") \
        .withColumn("patient_id", coalesce(col("id"), expr("uuid()"))) \
        .withColumn("name", from_json(col("name"), NAME_SCHEMA)) \
        .withColumn("name_exploded", explode_outer(col("name"))) \
        .withColumn("identifier_exploded", explode_outer(col("identifier"))) \
        .withColumn("telecom_exploded", explode_outer(col("telecom"))) \
        .withColumn("address_exploded", explode_outer(col("address")))
    
    df_patients = df_patients_raw.groupBy(
        col("patient_id"),
        initcap(col("gender")).alias("gender"),
        col("telecom_exploded.value").alias("phone_number"),
        col("address_exploded.line")[0].alias("address_line"),
        col("address_exploded.city").alias("city"),
        col("address_exploded.state").alias("state"),
        col("address_exploded.country").alias("country"),
        col("address_exploded.postalCode").alias("postal_code")
    ).agg(
        current_timestamp().alias("created_at"),
        max(expr("case when identifier_exploded.type.coding[0].code = 'SS' then identifier_exploded.value end")).alias("SSN"),
        first(
            initcap(
                regexp_replace(
                    concat_ws(" ",
                        coalesce(col("name_exploded.prefix").getItem(0), lit("")),
                        coalesce(col("name_exploded.given").getItem(0), lit("")),
                        col("name_exploded.family")
                    ),
                    "[^\\p{L}\\s.]", ""
                )
            )
        ).alias("full_name")
    )
    
    return df_patients.select(
        "patient_id", "SSN", "full_name", "gender", "phone_number",
        "address_line", "city", "state", "country", "postal_code", "created_at"
    )

def process_medications(df):
    DOSAGE_PATTERN = r"(\d+(\.\d+)?\s*(MG/ML|UNT/ML|MG|G|MCG|ML|HR|HOUR|MEQ|TABLET|CAPSULE|PATCH|INJECTION|SOLUTION|UNIT|UNT|IU))"
    ROUTE_PATTERN = r"(?i)(Oral Tablet|Injectable Suspension|Injection|Transdermal Patch|Oral Solution|Oral Capsule|Transdermal|Oral|IV|IM|SC|Topical|Sublingual|Buccal|Intranasal|Ophthalmic|Otic|Rectal|Vaginal)"
    FREQUENCY_PATTERN = r"(?i)\b(\d+\s*h(?:r)?)\b"
    
    df_med = df.filter(col("resourceType") == "MedicationRequest") \
        .withColumn("med_id", coalesce(col("id"), expr("uuid()"))) \
        .withColumn("administration_route", regexp_extract(col("medicationCodeableConcept.text"), ROUTE_PATTERN, 1)) \
        .withColumn("text_without_route", regexp_replace(col("medicationCodeableConcept.text"), ROUTE_PATTERN, "")) \
        .withColumn("medication_part", explode_outer(split(col("text_without_route"), " / ")))
    
    df_med = df_med.withColumn("frequency", trim(regexp_extract(col("medication_part"), FREQUENCY_PATTERN, 1))) \
                   .withColumn("frequency", when(col("frequency") == "", None).otherwise(col("frequency"))) \
                   .withColumn("medication_part", trim(regexp_replace(col("medication_part"), FREQUENCY_PATTERN, "")))
    
    df_med = df_med.withColumn("dosage", regexp_extract(col("medication_part"), DOSAGE_PATTERN, 1)) \
                   .withColumn("dosage", when(trim(col("dosage")) == "", None).otherwise(col("dosage")))
    
    df_med = df_med.withColumn("medication_name", trim(regexp_replace(col("medication_part"), DOSAGE_PATTERN, ""))) \
                   .withColumn("medication_name", trim(regexp_replace(col("medication_name"), ROUTE_PATTERN, ""))) \
                   .withColumn("medication_name", trim(regexp_replace(col("medication_name"), r"\[.*?\]", ""))) \
                   .withColumn("medication_name", regexp_replace(col("medication_name"), r"[^\p{L}\p{N}\s/\.]", "")) \
                   .withColumn("medication_name", initcap(col("medication_name"))) \
                   .withColumn("administration_route", initcap(col("administration_route")))
    
    df_med = df_med.withColumn("patient_ref", regexp_replace(col("subject.reference"), "urn:uuid:", "")) \
                   .withColumn("frequency", when(
                       (col("frequency").isNotNull()) & 
                       (col("frequency").endswith("h")) & 
                       (~col("frequency").endswith("hr")),
                       concat(col("frequency"), lit("r"))
                   ).otherwise(col("frequency")))
    
    
    return df_med.selectExpr(
        "med_id",
        "patient_ref",
        "medication_name",
        "dosage",
        "administration_route",
        "translate(requester.display, '0123456789', ' ') as prescribing_doctor",
        "status as prescription_status",
        "frequency"
    )

def process_conditions(df):
    df_cond = df.filter(col("resourceType") == "Condition") \
        .withColumn("condition_id", coalesce(col("id"), expr("uuid()"))) \
        .selectExpr(
            "condition_id",
            "regexp_replace(subject.reference, 'urn:uuid:', '') as patient_ref",
            "code.text as condition_description",
            "clinicalStatus.coding[0].code as condition_status",
        )
    
    df_cond = df_cond.withColumn(
        "condition_description",
        initcap(regexp_replace(regexp_replace(col("condition_description"), "(?i)\\(disorder\\)", ""), "[^\\p{L}\\p{N}\\s]", ""))
    )
    
    return df_cond

# Processa os dados
df_patients_final = process_patients(df)
df_medications_final = process_medications(df)
df_conditions_final = process_conditions(df)

# Configuração JDBC para ClickHouse (usando a porta HTTP, pois o container mapeia a porta 8123)
clickhouse_url = "jdbc:clickhouse://localhost:8123/healthcare"
connection_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "spark_user",
    "password": "qwe123!Q"
}

# Reparticiona e define batchsize para reduzir o uso de memória
df_patients_final.repartition(5).write.mode("append").option("batchsize", "1000").jdbc(url=clickhouse_url, table="patients", properties=connection_properties)
df_medications_final.repartition(5).write.mode("append").option("batchsize", "1000").jdbc(url=clickhouse_url, table="medications", properties=connection_properties)
df_conditions_final.repartition(5).write.mode("append").option("batchsize", "1000").jdbc(url=clickhouse_url, table="conditions", properties=connection_properties)

spark.stop()
print("Dados gravados com sucesso!")