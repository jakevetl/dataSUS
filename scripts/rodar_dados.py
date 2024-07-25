import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# vai iniciar uma sessão no spark
spark = SparkSession.builder \
    .appName("ProjetoDataScience") \
    .getOrCreate()

file_path = "C:/Users/johnn/Documents/workspace/Projetos/dataframes/EDA_Manipulados_Combinado.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)


df.printSchema()
df.show(5)

num_rows = df.count()
print(f"O total de linhas é de: {num_rows} linhas")

#Estatística descritiva básica
df.describe().show()

# parar a sessão do spark
spark.stop()
