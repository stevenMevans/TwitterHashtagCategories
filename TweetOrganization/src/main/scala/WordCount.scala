import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mustafamuswadi on 2/25/17.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Word Count").setMaster("local")

    val sparkContext = new SparkContext(conf)

    val inputFile = sparkContext.textFile("src/main/resources/sample.json")

    val counts = inputFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("src/main/resources/output")


  }

}
