import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaService {

  //kafka参数
  val bootstrapServers = "10.16.1.118:9095,10.16.1.119:9095,10.16.1.120:9095"

  def produceToKafka(data: String, topic : String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = data.split("\n")
    for (s <- dataArr) {
      if (s.trim.nonEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
//        println("[send]:" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }
}
