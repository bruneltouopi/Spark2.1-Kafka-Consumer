/**
  * Created by f.touopi.touopi on 1/9/2017.
  */
import org.apache.spark._
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
case  class Vote(candidate:Integer,date :String,voterId:String)

object ConsumerMsg {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("counterAppStream").setMaster("spark://169.254.12.172:7077")
    //.setMaster("local[*]")
      //.setMaster("spark://169.254.12.172:7077")
    // val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("checkpoint")

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("my-replicated-topic3")
    //names of the topic
    val brokers = "localhost:9092,localhost:9093,localhost:9094"
    //name of brokers if many concatane with ,
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams))
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)

    val votes=lines.map(_.split(";")).map(x=>(x(0),1)).reduceByKeyAndWindow((x: Int, y: Int) => x+y,
      (x: Int, y: Int) => x-y,
      Seconds(20),Seconds(10))

    votes.print()
    votes.saveAsTextFiles("hdfs://localhost:9000/user/hduser/output/" )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

