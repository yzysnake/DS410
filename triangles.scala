// Put your import statements here
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame


object DFlab {
    def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar
      val df = getOriginDataFrame()
      val counts = numOfTriangles(df)
      saveIt(counts,"FacebookTriangles")
  }

    //create a test rdd, put in the correct types and create a datafrane
    def getTestDataFrame() = {
      List(("A", "B"),
                   ("B", "A"),
                   ("A", "C"),
                   ("B", "C"),
                   ("A", "D"),
                   ("C", "E"),
                   ("D", "C"),
                   ("C", "A"),
                   ("C", "B"),
                   ("D", "A"),
                   ("C", "D"),
                   ("E", "C")).toDF()

    }

    def getOriginDataFrame() = {
      val mySchema = new StructType().add("NODE1", StringType, true).add("NODE2", StringType, true)
      val df2 = spark.read.format("csv").option("header", "false").option("delimiter", " ").schema(mySchema).load("/datasets/facebook")
    df2

    }

    def numOfTriangles(graph: DataFrame) : DataFrame = {
      val flipped = graph.select("NODE2", "NODE1")
      val combined = graph.union(flipped).distinct()
      val selfjoin = combined.as("c1").join(combined.as("c2"), col("c1.NODE2") === col("c2.NODE2")).select("c1.NODE2","c1.NODE1","C2.NODE1")
      val cleaned = selfjoin.where(col("c1.NODE1") =!= col("c2.NODE1"))
      val flipped_clean = cleaned.select("c1.NODE1","c2.NODE1", "c1.NODE2")
      val hacked_combined = combined.withColumn("number", expr("1"))
      val all_joined = flipped_clean.join(hacked_combined.as("temp"), col("c1.NODE1") === col("temp.NODE1")
        && col("c2.NODE1") === col("temp.NODE2"))
      val final_df = all_joined.select(expr("INT(sum(number) / 6)").as("Triangles"))

    final_df
    }

    def saveIt(answer: DataFrame, name: String) {
      answer.write.format("csv")
      .option("mode", "ignore")
      .save(name)

    }



}

