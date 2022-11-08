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
    val input = sc.textFile("/datasets/retailtab")
    val words = input.map(x => x.split("\t"))
    val new_words = words.filter(x => x(0) != "InvoiceNo").filter(x => x(3).toDouble >= 0.0)
    val kv =  new_words.map(word => (word(7),word(5).toDouble * word(3).toDouble))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }
  def saveit(counts: org.apache.spark.rdd.RDD[(String, Double)]) = {
    counts.saveAsTextFile("sparklab_q3")
  }
}