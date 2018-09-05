import org.apache.spark.sql.SparkSession

//Criando uma sessão do Spark
val spark = SparkSession.builder().getOrCreate()

//Lendo arquivo csv(Conjunto de Dados de informações financeiras imobiliárias)
//option("header","true") é um modo de "nomear as colunas" de maneira correta
//option("inferSchema","true") é para o programa inferir o esquema e atriubir o tipo correto de cada, 
//pois se não ia tratar tudo como string
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

df.printSchema()

//importando lib pra usar a notação do Scala
import spark.implicits._

/* Sabemos que em Spark temos transformoçãoes e ações, e no caso filter é a transformação 
e show/collect/count são as ações */

//sintaxe do Scala
println("Filter com sintaxe Scala")
println("--------------------------------------------------------")
println("Desigualdade")
df.filter($"Close" > 480).show()
println("--------------------------------------------------------")
println("Igualdade")
df.filter($"High" === 484.40).show()

//sintaxe SQL
//filter funciona como um "where"
println("Filter com sintaxe SparkSQL")
println("--------------------------------------------------------")
println("Desigualdade")
df.filter("Close > 480").show()
println("--------------------------------------------------------")
println("Igualdade")
df.filter("High = 484.40").show()

//filter em mais de uma coluna Scala
println("--------------------------------------------------------")
println("Filter em mais de uma coluna com sintaxe Scala")
println("--------------------------------------------------------")
println("AND")
df.filter($"Close" < 480 && $"High" < 480).show()
println("--------------------------------------------------------")
println("OR")
df.filter($"Close" < 480 || $"High" < 480).show()

//filter em mais de uma coluna SQL
println("--------------------------------------------------------")
println("Filter em mais de uma coluna com sintaxe SparkSQL")
println("--------------------------------------------------------")
println("AND")
df.filter("Close < 480 AND High < 480").show()
println("--------------------------------------------------------")
println("OR")
df.filter("Close < 480 OR High < 480").show()




//Pegamos o resultado das consultas abaixo e transformamos em arrays para a utilização dos dados
println("--------------------------------------------------------")
val CloseandHigh = df.filter("Close < 480 AND High < 480").collect()

//armazenamos o resultado de quantas linhas nossa consulta retornou
println("--------------------------------------------------------")
val CloseandHighCount = df.filter("Close < 480 AND High < 480").count()
println("--------------------------------------------------------")
println("O número de linhas retornado pela consulta é: ", CloseandHighCount)

//Como obter a correlação entre duas colunas
println("--------------------------------------------------------")
println("a correlação entre a Coluna High e Low")
df.select(corr("High","Low")).show()



