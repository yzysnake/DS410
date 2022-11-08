import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object WordCount {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = doWordCount(sc)
    saveit(counts)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
  }
  def doWordCount(sc: SparkContext) = {
    val input = sc.textFile("/datasets/wap")
    val words = input.flatMap(_.split(" ")).filter(x => x.length > 2)
    val kv =  words.map(word => (word.length().toString,1))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }
  def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
    counts.saveAsTextFile("sparklab_q2")
  }
}
