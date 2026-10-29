import os
import matplotlib
matplotlib.use("Agg")  # guardar gráficas sin pantalla
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.expanduser("~/bigdata_batch")
CSV = "file://" + BASE + "/datos/covid_colombia.csv"
LIMPIO = "file://" + BASE + "/datos/covid_limpio"
RES = BASE + "/resultados"
os.makedirs(RES, exist_ok=True)

spark = (SparkSession.builder.appName("AnalisisCovidColombia")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- 1. CARGA desde la fuente original ----------
columnas = ["fecha_reporte", "id_caso", "fecha_notificacion", "cod_depto",
            "departamento", "cod_municipio", "municipio", "edad", "unidad_edad",
            "sexo", "tipo_contagio", "ubicacion", "estado", "cod_pais", "pais",
            "recuperado", "fecha_sintomas", "fecha_muerte", "fecha_diagnostico",
            "fecha_recuperacion", "tipo_recuperacion", "pertenencia_etnica",
            "grupo_etnico"]

raw = (spark.read.option("header", True).option("quote", '"')
       .option("escape", '"').csv(CSV).toDF(*columnas))
total_crudo = raw.count()
print(f"Filas en el archivo original: {total_crudo}")

# ---------- 2. LIMPIEZA Y TRANSFORMACIÓN ----------
df = (raw
      .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
      .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
      .withColumn("edad", F.col("edad").cast("int"))
      .withColumn("unidad_edad", F.col("unidad_edad").cast("int"))
      # edad en años: 1 = años, 2 = meses, 3 = días
      .withColumn("edad_anios",
                  F.when(F.col("unidad_edad") == 1, F.col("edad"))
                   .when(F.col("unidad_edad") == 2, F.floor(F.col("edad") / 12))
                   .when(F.col("unidad_edad") == 3, F.lit(0)))
      .withColumn("fecha_notificacion",
                  F.to_date(F.substring("fecha_notificacion", 1, 10)))
      .withColumn("recuperado", F.initcap(F.trim(F.col("recuperado"))))
      .withColumn("estado", F.initcap(F.trim(F.col("estado")))))

df = (df.filter(F.col("id_caso").isNotNull())
        .dropDuplicates(["id_caso"])
        .filter(F.col("departamento").isNotNull() & (F.col("departamento") != ""))
        .filter(F.col("sexo").isin("F", "M"))
        .filter(F.col("edad_anios").between(0, 110))
        .filter(F.col("fecha_notificacion").isNotNull())
        .withColumn("mes", F.date_format("fecha_notificacion", "yyyy-MM"))
        .withColumn("fallecido",
                    F.when((F.col("recuperado") == "Fallecido") |
                           (F.col("estado") == "Fallecido"), 1).otherwise(0))
        .withColumn("grupo_edad",
                    F.least(F.floor(F.col("edad_anios") / 10), F.lit(8)))
        .select("id_caso", "fecha_notificacion", "mes", "departamento",
                "municipio", "edad_anios", "grupo_edad", "sexo",
                "tipo_contagio", "estado", "recuperado", "fallecido"))

# Guardar los datos limpios en Parquet y leerlos de nuevo (más rápido y liviano)
df.write.mode("overwrite").parquet(LIMPIO)
df = spark.read.parquet(LIMPIO)

total = df.count()
print(f"Filas después de la limpieza: {total}")

# ---------- 3. ANÁLISIS EXPLORATORIO (EDA) ----------
por_depto = (df.groupBy("departamento").count()
               .orderBy(F.desc("count")).limit(10).toPandas())
por_mes = df.groupBy("mes").count().orderBy("mes").toPandas()
por_sexo = df.groupBy("sexo").count().toPandas()
por_edad = (df.groupBy("grupo_edad")
              .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
              .orderBy("grupo_edad").toPandas())

etiquetas = ["0-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+"]
por_edad["rango"] = por_edad["grupo_edad"].map(lambda i: etiquetas[int(i)])
por_edad["letalidad_pct"] = por_edad["fallecidos"] / por_edad["casos"] * 100
total_fallecidos = int(por_edad["fallecidos"].sum())

print("\nTop 10 departamentos:\n", por_depto.to_string(index=False))
print("\nPor sexo:\n", por_sexo.to_string(index=False))
print("\nPor grupo de edad:\n", por_edad[["rango", "casos", "fallecidos", "letalidad_pct"]].to_string(index=False))

# ---------- 4. VISUALIZACIÓN ----------
def guardar(nombre):
    plt.tight_layout()
    plt.savefig(f"{RES}/{nombre}", dpi=120)
    plt.close()

plt.figure(figsize=(10, 6))
plt.barh(por_depto["departamento"][::-1], por_depto["count"][::-1])
plt.title("Top 10 departamentos con más casos de COVID-19")
plt.xlabel("Casos")
guardar("casos_por_departamento.png")

plt.figure(figsize=(12, 5))
plt.plot(por_mes["mes"], por_mes["count"])
plt.title("Casos de COVID-19 por mes")
plt.xlabel("Mes")
plt.ylabel("Casos")
plt.xticks(range(0, len(por_mes), 3), por_mes["mes"][::3], rotation=45)
guardar("casos_por_mes.png")

plt.figure(figsize=(6, 5))
plt.bar(por_sexo["sexo"], por_sexo["count"], color=["#d9627d", "#4c8bbf"])
plt.title("Casos por sexo")
plt.ylabel("Casos")
guardar("casos_por_sexo.png")

plt.figure(figsize=(10, 5))
plt.bar(por_edad["rango"], por_edad["casos"])
plt.title("Casos por grupo de edad")
plt.xlabel("Edad (años)")
plt.ylabel("Casos")
guardar("casos_por_edad.png")

plt.figure(figsize=(10, 5))
plt.bar(por_edad["rango"], por_edad["letalidad_pct"], color="#c0392b")
plt.title("Letalidad por grupo de edad (% de casos fallecidos)")
plt.xlabel("Edad (años)")
plt.ylabel("%")
guardar("letalidad_por_edad.png")

# ---------- 5. RESUMEN EN TEXTO ----------
with open(f"{RES}/resumen_resultados.txt", "w") as f:
    f.write(f"Filas del archivo original: {total_crudo}\n")
    f.write(f"Filas despues de la limpieza: {total}\n")
    f.write(f"Total de fallecidos: {total_fallecidos}\n\n")
    f.write("Top 10 departamentos:\n" + por_depto.to_string(index=False) + "\n\n")
    f.write("Por grupo de edad:\n" + por_edad[["rango", "casos", "fallecidos", "letalidad_pct"]].to_string(index=False) + "\n")

print("\nListo. Resultados guardados en:", RES)
spark.stop()