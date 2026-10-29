# Análisis batch de casos de COVID-19 en Colombia con Apache Spark

Tarea 3 – Procesamiento de Datos con Apache Spark
Curso: Big Data (código 202016911) – Universidad Nacional Abierta y a Distancia (UNAD)
Grupo: 202016911_21
Autor: Teddys Stevan Jimenez Julio

## Descripción del problema

¿Cómo se distribuyeron los casos de COVID-19 en Colombia por departamento, sexo, grupo de edad y tiempo, y qué grupos presentaron mayor letalidad?

Para responderlo se desarrolló una aplicación en Python con Apache Spark (PySpark) que procesa en modo batch el registro nacional de casos positivos.

## Conjunto de datos

- **Nombre:** Casos positivos de COVID-19 en Colombia
- **Fuente:** Portal de Datos Abiertos de Colombia (datos.gov.co), identificador `gt2j-8ykr`
- **Enlace:** https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr/data
- **Tamaño:** aprox. 1,2 GB en CSV, con millones de registros
- **Campos principales:** fecha de notificación, departamento, municipio, edad, unidad de medida de la edad, sexo, tipo de contagio, estado y recuperación.

El archivo **no se incluye en este repositorio** por su tamaño. Hay que descargarlo (ver instrucciones).

## Qué hace el código (`analisis_covid.py`)

1. **Carga** el CSV desde la fuente original con Spark.
2. **Limpieza y transformación** con DataFrames: elimina duplicados por `id_caso`, normaliza texto, convierte la edad a años, descarta sexos y edades inválidos, convierte fechas y crea las columnas `mes`, `grupo_edad` y `fallecido`. Guarda el resultado limpio en formato Parquet.
3. **Análisis exploratorio (EDA):** casos por departamento, por mes, por sexo y por grupo de edad, y letalidad por grupo de edad.
4. **Visualización:** genera gráficas con matplotlib y un resumen en texto.

## Requisitos

- Ubuntu 22.04 (probado en una máquina virtual con VirtualBox)
- Java 11
- Python 3.10
- Apache Spark 3.5.3
- Librerías de Python: `pandas` y `matplotlib`

## Instrucciones de ejecución

```bash
# 1. Clonar el repositorio
git clone https://github.com/ted12478/covid-colombia-spark.git
cd covid-colombia-spark

# 2. Instalar las librerías de Python
pip install pandas matplotlib

# 3. Descargar el dataset en la carpeta datos/
mkdir -p datos resultados
wget -O datos/covid_colombia.csv "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"

# 4. Ejecutar el análisis
spark-submit --driver-memory 2g analisis_covid.py
```

El procesamiento tarda varios minutos por el tamaño del archivo. Se recomienda una máquina con al menos 4 GB de RAM.

Nota: el script usa la ruta `~/bigdata_batch`. Si clonas el repositorio en otra carpeta, ajusta la variable `BASE` al inicio de `analisis_covid.py`.

## Resultados

Las salidas se guardan en la carpeta `resultados/`:

| Archivo | Contenido |
|---|---|
| `casos_por_departamento.png` | Top 10 de departamentos con más casos |
| `casos_por_mes.png` | Evolución de los casos por mes |
| `casos_por_sexo.png` | Casos por sexo |
| `casos_por_edad.png` | Casos por grupo de edad |
| `letalidad_por_edad.png` | Porcentaje de fallecidos por grupo de edad |
| `resumen_resultados.txt` | Resumen numérico del análisis |

### Hallazgo principal

La letalidad aumenta con la edad: pasa de cerca del 0,1 % en menores de 10 años a más del 22 % en mayores de 80 años. Los grupos de 20 a 49 años concentran la mayor cantidad de casos.

## Estructura del repositorio

```
.
├── analisis_covid.py
├── README.md
├── .gitignore
├── datos/        (vacía; aquí se descarga el CSV)
└── resultados/   (gráficas y resumen generados)
```

## Procesamiento en tiempo real

La parte de tiempo real (Apache Kafka y Spark Streaming, Anexo 3) se realizó aparte y se muestra en el video de la actividad.