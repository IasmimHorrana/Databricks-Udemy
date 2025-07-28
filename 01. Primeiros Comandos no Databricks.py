# Databricks notebook source
# MAGIC %md
# MAGIC ##### DBFS e dbutils Documentação 
# MAGIC https://docs.databricks.com/pt/dbfs/index.html
# MAGIC
# MAGIC https://docs.databricks.com/pt/dev-tools/databricks-utils.html
# MAGIC
# MAGIC ##### Arquitetura Azure
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/architecture/solution-ideas/articles/ingest-etl-stream-with-adb

# COMMAND ----------

# Mostra todos os módulos disponíveis dentro do dbutils
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Principais comandos do dbutils.fs
# MAGIC | Comando                  | Descrição                                |
# MAGIC | ------------------------ | ---------------------------------------- |
# MAGIC | `ls(path)`               | Lista os arquivos e pastas do caminho    |
# MAGIC | `cp(from, to)`           | Copia arquivos ou pastas                 |
# MAGIC | `mv(from, to)`           | Move arquivos ou pastas                  |
# MAGIC | `rm(path, recurse=True)` | Remove arquivos/pastas                   |
# MAGIC | `mkdirs(path)`           | Cria diretórios                          |
# MAGIC | `head(file)`             | Mostra os primeiros bytes de um arquivo  |
# MAGIC | `put(path, contents)`    | Cria/escreve um arquivo de texto simples |
# MAGIC

# COMMAND ----------

# Lista o conteúdo da raiz do sistema de arquivos
dbutils.fs.ls('/')

# COMMAND ----------

# Lista arquivos dentro de um volume do Unity Catalog
display(dbutils.fs.ls('/Volumes/workspace/default/udemy-course/arquivos_csv/'))

# COMMAND ----------

# Visualiza os primeiros bytes de um arquivo CSV
dbutils.fs.head('/Volumes/workspace/default/udemy-course/arquivos_csv/Clientes.csv')

# COMMAND ----------

# Lista de arquivos a serem excluídos (exemplo)
excluir = [
    "/FileStore/categories.csv",
    "/FileStore/customers.csv",
    "/FileStore/order_items.csv",
    "/FileStore/orders.csv",
    "/FileStore/products.csv",
    "/FileStore/staffs.csv",
    "/FileStore/stocks.csv",
    "/FileStore/stores.csv",
]

# Loop para deletar os arquivos
for apagar_arquivos in excluir:
    dbutils.fs.rm(apagar_arquivos)

# COMMAND ----------

# Visualizar diretórios existentes no FileStore
display(dbutils.fs.ls('/Volumes/workspace/default/udemy-course/'))

# COMMAND ----------

# Criar uma nova pasta dentro do FileStore
dbutils.fs.mkdirs('/FileStore/tables/Bikes')

# COMMAND ----------

# Verificar se a nova pasta foi criada com sucesso
display(dbutils.fs.ls('/FileStore/tables/Bikes'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Visualizando os dados

# COMMAND ----------

dbfs:/FileStore/tables/Bikes/customers.csv
/dbfs/FileStore/tables/Bikes/customers.csv

# COMMAND ----------

# Ler o arquivo CSV e criar um DataFrame
spark.read.csv("/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True)


# COMMAND ----------

display(spark.read.csv("/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True))
#header = primeira linha é o cabeçalho?
#inferSchema = inserir automaticamente os tipos de dados(numero,texto,data etc....)

# COMMAND ----------

#salvando dados em um DF(DataFlame)
df = spark.read.csv("dbfs:/FileStore/tables/Bikes/customers.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Tipos de visualizações 

# COMMAND ----------

df

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

#Ver algumas colunas sem salvar em um DF
colunas = ["customer_id", "first_name", "email", "state"]
display(df.select(colunas))

# COMMAND ----------

df.select('customer_id','email').show()

# COMMAND ----------

display(df.select('customer_id','email'))   

# COMMAND ----------

#salvando em um novo DF ou subscrevendo
colunas = ["customer_id", "first_name", "email", "state"]
dfFiltrado = df.select(colunas)

# COMMAND ----------

dfFiltrado.show(5)

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Sobrescrevendo DF

# COMMAND ----------

df =  df.select('first_name','email','city')

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Localizando Base de Dados Para Treino
# MAGIC
# MAGIC https://www.kaggle.com

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/wine-quality/'))

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/wine-quality/winequality-white.csv',header=True , inferSchema=True , sep=';').show(5)

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/COVID/coronavirusdataset/'))

# COMMAND ----------

Arquivo ="dbfs:/databricks-datasets/COVID/coronavirusdataset/SearchTrend.csv"
spark.read.csv(Arquivo,header=True , inferSchema=True ).show(5)

