//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//
//object WordCount {
//
//  def main(args: Array[String]) {
//    val inputFile =  "file:///Users/xyj/developer/logs/word2.txt"
//    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
//    val sc = new SparkContext(conf)
//    val textFile = sc.textFile(inputFile,1)
//    val wordCount = textFile.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    wordCount.foreach(println)
//
//  }
//}
