import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.mongodb.spark.MongoConnector

object InvertedIndex {

  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark Context
    val conf = new SparkConf()
      .setAppName("InvertedIndex")
      .setMaster("local[*]")
      .set("spark.mongodb.output.uri", "mongodb://localhost:27017/InvertedIndex.dictionary")

    val sc = new SparkContext(conf)

    val documents = sc.wholeTextFiles("/Users/mbair/Downloads/untitled2 2/src/main/wholeInvertedIndex.txt")

    val wordsInDocs = documents.flatMap {
      case (docPath, content) =>
        val docName = docPath.split("/").last
        content.split("\\W+").map(word => (word.toLowerCase, docName))
    }

    val wordDocPairs = wordsInDocs.distinct()

    val invertedIndex = wordDocPairs
      .groupByKey()
      .mapValues { docs =>
        val docList = docs.toList.distinct.sorted
        (docList.size, docList)
      }

    val mongoDocuments: RDD[Document] = invertedIndex.map {
      case (word, (count, docList)) =>
        new Document("word", word)
          .append("count", count)
          .append("documents", docList)
    }

MongoSpark.save(mongoDocuments)

    sc.stop()
  }
}
