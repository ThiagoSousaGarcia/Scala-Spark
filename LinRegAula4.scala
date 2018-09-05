//importando libs pra usar algortimos de regressão linear
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

//importando libs para mostrar melhor os erros ao compilar
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

//iniciando uma sessão spark
val spark = SparkSession.builder().getOrCreate()

//lendo os dados
// a partir das informações da casa, queremos predizer o preço da casa
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Clean-USA-Housing.csv")

data.printSchema()

/*temos que transformar os dados em data frame do tipo ("label","features"), 
ou seja teremos uma coluna de label e outra coluna com as features
*/

/* Para fazer a transformação desses dados nesse tipo de DataFrame, 
iremos precisar da biblioteca VectorAssembler e da também da biblioteca vectors
*/
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

/*olhamos quais são as colunas de tipo numérico, pois são elas que iremos utilizar*/
data.columns

/* Criamos um novo dataframe a partir de data, onde o preço vai ser nosso label 
e os demais valores númericos de data serão nossas features
*/
val df = (data.select(data("Price").as("label"),
	$"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", 
	$"Avg Area Number of Bedrooms", $"Area Population"))

df.printSchema()

/*Estamos colocando todas nossas features em um array para que possamos transforma-las em uma única coluna chamada features no nosso
DataSet */
val assembler = (new VectorAssembler()
	.setInputCols(Array("Avg Area Income","Avg Area House Age","Avg Area Number of Rooms","Avg Area Number of Bedrooms","Area Population"))
	.setOutputCol("features"))	

val output = assembler.transform(df).select($"label",$"features")

output.show()

/* Agora iremos criar nosso modelo de regressão linear, treiná-lo e depois explorar os resultados*/
val lr = new LinearRegression()

/*treinamos nossos dados*/
val lrModel = lr.fit(output)

/*obtemos um sumário dos resultados do nosso treinamento*/
val trainingSummary = lrModel.summary

/*Aqui obtemos um dataframe com a coluna do valor que foi predito a partir das features e do modelo de regressão linear*/
trainingSummary.predictions.show()

/*Aqui obtemos um datafram com os valores residuais que consiste da diferença entre o valor do label e o valor predito a partir das features*/
trainingSummary.residuals.show()