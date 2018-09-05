import org.apache.spark.sql.SparkSession

//Criando uma sessão do Spark
val spark = SparkSession.builder().getOrCreate()

//Lendo arquivo csv(Conjunto de Dados de informações financeiras mobiliárias)
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

//printando o esquema
println("Printando o esquema")
println("---------------------------------------")
df.printSchema()

println("Mostrando os dados")
println("---------------------------------------")
df.show()

println("Extraindo o dia do ano da coluna Date")
println("---------------------------------------")
df.select(dayofyear(df("Date"))).show()

println("Extraindo o dia do mês da coluna Date")
println("---------------------------------------")
df.select(dayofyear(df("Date"))).show()

println("Extraindo o mês da coluna Date")
println("---------------------------------------")
df.select(month(df("Date"))).show()

println("Extraindo o ano da coluna Date")
println("---------------------------------------")
df.select(year(df("Date"))).show()

println("Extraindo a hora da coluna Date")
println("---------------------------------------")
df.select(hour(df("Date"))).show()

println("Extraindo o minuto da coluna Date")
println("---------------------------------------")
df.select(minute(df("Date"))).show()

println("Criando um novo DataFrame com uma coluna com os anos")
println("---------------------------------------")
val df2 = df.withColumn("Year",year(df("Date")))

df2.show()

println("Criando um novo DataFrame com as médias de preço por ano")
println("---------------------------------------")
val dfavgs = df2.groupBy("Year").mean()

dfavgs.show()

println("Selecionando a média do maior preço por ano")
println("---------------------------------------")
dfavgs.select($"Year",$"avg(High)").show()

println("Criando um novo DataFrame com os menores preço por ano")
println("---------------------------------------")
val dfmin = df2.groupBy("Year").min()

dfmin.show()

println("Selecionando o menor preço dos maiores preços por ano")
println("---------------------------------------")
dfmin.select($"Year",$"min(High)").show()
