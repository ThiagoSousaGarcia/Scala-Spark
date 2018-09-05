// Iniciando uma sessão Spark
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

//Lendo csv e armazenando em um Dataframe
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")

//printando o esquema
println("Printando o esquema")
println("-------------------------------------------")
df.printSchema()

//mostrando os dados, podemos perceber que há muitos dados nulos
println("Mostrando os dados")
println("-------------------------------------------")
df.show()

//retira do Dataframe qualquer linha que contenha dados nulos
println("Retirando todos as linhas que contém dados nulos")
println("-------------------------------------------")
df.na.drop().show()
//se passarmos um inteiro como argumento, ele retira todas aquelas linhas 
//que possuem menos que essa quantidade em valores não nulos
println("Retirando todos as linhas que contém menos que um valor inteiro(3, no caso) de valores não nulos")
println("-------------------------------------------")
df.na.drop(3).show()

//preenche os dados do Dataframe, dependendo do tipo do dado que foi passado em fill()
//ele preenche exatamente as colunas que possuem esse tipo de dados
println("Preenchendo a coluna númerica com o valor 100")
println("-------------------------------------------")
df.na.fill(100).show()
println("Preenchendo a coluna de String com o valor Nome Ausente ")
println("-------------------------------------------")
df.na.fill("Nome Ausente").show()

//Caso quisermos preencher uma coluna especifica, fazemos da maneira abaixo
println("Preenchendo a coluna Name com o valor Nome Ausente")
println("-------------------------------------------")
df.na.fill("Nome Ausente",Array("Name")).show()
println("Preenchendo a coluna Sales com o valor 100")
println("-------------------------------------------")
df.na.fill(100,Array("Sales")).show()

//para preencher as duas colunas ao mesmo tempo, preenchemos a primeira e armazenamos em uma variavél
//e depois preenchemos a outra
val df2 = df.na.fill("Nome Ausente",Array("Name"))
println("Mostrando dados importantes da tabela para que possamos pegar o valor da média")
println("-------------------------------------------")
df2.describe().show()
println("Preenchendo a coluna Sales com o valor 400.5(valor da média)")
println("-------------------------------------------")
df2.na.fill(400.5,Array("Sales")).show()

/* NÃO CONSEGUI PEGAR A MEDIA DESSA FORMA, POIS MEDIA É UM OBJETO DO TIPO DATAFRAME 
E NÃO CONSEGUI CONVERTER PRA DOUBLE E PASSAR EM FILL()
val media = df2.select(avg("Sales"))
df2.na.fill(media,Array("Sales")).show()*/



