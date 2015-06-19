import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.util.Vector
import java.lang.Math

object KMeans {
  
  def FindCluster(p:Pair[Double,Double], centers: Array[Pair[Double,Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for(i <- 0 until centers.length){
      var xDist= (p._1-centers(i)._1)*(p._1-centers(i)._1)
      var yDist= (p._2-centers(i)._2)*(p._2-centers(i)._2)
      var tempDist=Math.sqrt(xDist+yDist)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }
  
  def main(args: Array[String]) {
    //get the input file
    var dataFile ="src/AlFiles/Q1_testkmean.txt"
    // initialize the spark context
    var sc = new SparkContext("local", "KMeans App", "/path/to/spark-0.9.1-incubating",List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    var logData = sc.textFile(dataFile, 2).map(line => (line.split(" ")(0).toDouble, line.split(" ")(1).toDouble))
    logData.saveAsTextFile("src/data/logData")
    
    //read k value i.e. 3
    var k=3
    var bool=0
    var centroids = logData.takeSample(false, k, 42)
    // get number of iterations till 10
    while(bool<10)
    {
      bool=bool+1
      println("Iter number: " + bool)
      for(i <- 0 until centroids.length){
        println(centroids(i)._1 + "-->" + centroids(i)._2)
      }
      // get closest centroid
      var closestCentroid = logData.map (p => (FindCluster(p, centroids), (p,1)))
      var pointStats = closestCentroid.reduceByKey{case (((x1, y1),z1), ((x2, y2),z2)) => ((x1 + x2, y1 + y2),(z1+z2))}
      pointStats.saveAsTextFile("src/data/pointStats")
      
      var newPoints = pointStats.map {pair => ((pair._2._1._1 / pair._2._2),(pair._2._1._2 / pair._2._2))}.toArray
      
      for (j <- 0 until newPoints.size) {
        centroids(j)= newPoints(j) 
      }
    }
  } 
}