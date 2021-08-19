import org.apache.kafka.clients.consumer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies , KafkaUtils , LocationStrategies}
import org.apache.spark.streaming.{Seconds , StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object streaming_kafka_flutura{
  def main(args: Array[String]) : Unit = {

    val broker_id = "localhost:9092"
    val groupid = "GRP1"
    val topics = "testtopic"

    val topicset = topics.split(",").toSet
    val kafkaParams  = Map[String,Object] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid ,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf = new SparkConf().setMaster("local[2]").setAppName("kafka_spark_integration")

    val ssc = new StreamingContext(sparkconf , Seconds(5))

    val sc=ssc.sparkContext
    sc.setLogLevel("OFF")

    val message = KafkaUtils.createDirectStream[String , String](
      ssc ,
      LocationStrategies.PreferConsistent ,
      ConsumerStrategies.Subscribe[String , String](topicset ,kafkaParams )
    )

    val words = message.map(_.value()).flatMap(_.split(" "))


    val wordcount  = words.map(x => (x,1)).reduceByKey(_ +_)

    wordcount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}