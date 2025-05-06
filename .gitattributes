from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

# Criar sessão Spark
spark = SparkSession.builder.appName("LeituraCSV").getOrCreate()

# Leitura simples (sem schema e sem header)
df = spark.read.csv('videos-stats.csv')

# Visualiza os primeiros 8 registros
df.show(8)

# Visualiza o esquema do DataFrame
df.printSchema()

# Leitura com header
df = spark.read.option('header', 'true').csv('videos-stats.csv')
df.printSchema()  # Ver esquema sem inferência

# Leitura com inferência de tipos
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv('videos-stats.csv')
df.printSchema()  # Ver esquema com inferência

# Mostra os dados do DataFrame lido
df.show(8)

# Salva o DataFrame em formato Parquet (binário, mais eficiente e cabeçalho)
df.write.option("header", "true").mode("overwrite").parquet('output/videos-parquet')

# Ler novamente o parquet com cabeçalho
df_parquet = spark.read.option("header", "true").parquet('output/videos-parquet')
df_parquet.show(5)

# Salvar como tabela no Spark catalog
df_parquet.write.mode("overwrite").saveAsTable("tb_videos")

# Verificar as tabelas do Spark
spark.catalog.listTables()

# Ler a tabela usando Spark SQL
df_sql = spark.sql("SELECT * FROM tb_videos")
df_sql.show(5)

# Ler o arquivo comments.csv com header e inferência de tipos
df_comments = spark.read.option('header', 'true').option('inferSchema', 'true').csv('comments.csv')
df_comments.show(5)
df_comments.printSchema()

# Salvar o DataFrame de comentários como parquet
df_comments.write.option("header", "true").mode("overwrite").parquet('output/comments-parquet')
