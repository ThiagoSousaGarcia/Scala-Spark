// iniciando a Spark Session
import org.apache.spark.sql.SparkSession

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Spark Session
val spark = SparkSession.builder().getOrCreate()

// Importando Algoritmo de clustering
import org.apache.spark.ml.clustering.KMeans

// carregando o dado
val dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")

// treinando o modelo kmeans
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(dataset)

// Avaliando a clusterização pela soma dos erros dos quadrados
val WSSSE = model.computeCost(dataset)
println(s"Within Set Sum of Squared Errors = $WSSSE")

//Mostrando resultados
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
