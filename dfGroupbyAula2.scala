// Iniciando uma sessão Spark
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

//Lendo csv e armazenando em um Dataframe
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

// Mostrar o esquema
df.printSchema()
df.show()

// Groupby 
df.groupBy("Company")

// Média		
df.groupBy("Company").mean().show()
// Count
df.groupBy("Company").count().show()
// Max
df.groupBy("Company").max().show()
// Min
df.groupBy("Company").min().show()
// Soma
df.groupBy("Company").sum().show()

// Outras funções de agregação
// Mais funções podem ser vistas em: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
df.select(countDistinct("Sales")).show() //approxCountDistinct
df.select(sumDistinct("Sales")).show()
df.select(variance("Sales")).show()
df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
df.select(collect_set("Sales")).show()

// OrderBy
// Ordem crescente
println("--------------------------------------------------------")
println("Coluna Sales na ordem crescente")
df.orderBy("Sales").show()

// Ordem decrescente
println("--------------------------------------------------------")
println("Coluna Sales na ordem decrescente") 
df.orderBy($"Sales".desc).show()
