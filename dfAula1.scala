import org.apache.spark.sql.SparkSession

//Criando uma sessão do Spark
val spark = SparkSession.builder().getOrCreate()

//Lendo arquivo csv(Conjunto de Dados de informações financeiras mobiliárias)
//option("header","true") é um modo de "nomear as colunas" de maneira correta
//option("inferSchema","true") é para o programa inferir o esquema e atriubir o tipo correto de cada, 
//pois se não ia tratar tudo como string


//val df = spark.read.csv("CitiGroup2006_2008")

//val df = spark.read.option("header","true").csv("CitiGroup2006_2008")


val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

//pegando 5 primeiras linhas do DataFrame
//df.head(5)

//imprimindo as 5 primeiras linhas do DataFrame
for( row <- df.head(5)) {
	println(row)
}



//mostra quais as colunas do DataFrame
println ("as colunas do DataFrame são:")
df.columns

//mostra informações importantes das colunas númericas do DataFrame
df.describe().show()


//fazer seleção de UMA coluna do DataFrame
df.select("Volume").show()


//fazer seleção de MAIS DE UMA coluna do DataFrame
df.select($"Volume",$"Date").show()


//criar uma nova coluna baseada em colunas anteriores
val df2 = df.withColumn("HighplusLow",df("High")+df("Low"))


df2.printSchema()

df2.head(5)

df2.describe().show()

//Renomear uma coluna
df2.select(df2("HighplusLow").as("HPL")).show()
