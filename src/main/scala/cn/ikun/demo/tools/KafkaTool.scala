package cn.ikun.demo.tools


import java.util.Properties

import cn.ikun.demo.tools.KafkaTool.createAdmin

import scala.beans.BeanProperty
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.concurrent.ExecutionException


object KafkaTool {

    /**
     * 创建Kafka消费者
     */
    def createConsumer: KafkaConsumer[String, String] = {
        val props = new Properties
        //声明kafka的地址
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        props.put("bootstrap.servers", PropertiesUtil.get("kafka.bootstrap-servers"))
        //每个消费者分配独立的消费者组编号
        props.put("group.id", PropertiesUtil.get("kafka.dau.consumer.group-id"))
        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", PropertiesUtil.get("kafka.dau.consumer.enable-auto-commit"))
        //自动重置offset
        props.put("auto.offset.reset", PropertiesUtil.get("kafka.dau.consumer.auto-offset-reset"))
        props.put("value.deserializer", PropertiesUtil.get("kafka.dau.consumer.value-deserializer"))
        props.put("key.deserializer", PropertiesUtil.get("kafka.dau.consumer.key-deserializer"))
        new KafkaConsumer[String, String](props)
    }

    /**
     *
     *  创建Kafka集群管理员对象
     */
    def createAdmin(servers: String): AdminClient = {
        val props = new Properties
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
        AdminClient.create(props)
    }

    /**
     *
     *  创建Kafka集群管理员对象
     */
    def createAdmin(): AdminClient = {
        createAdmin(PropertiesUtil.get("kafka.bootstrap-servers"))
    }

    /**
     *
     * 创建Kafka生产者
     */

    def createProducer: KafkaProducer[String, String] = {
        val props = new Properties
        //声明kafka的地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.get("kafka.bootstrap-servers"))
        //0、1 和 all：0表示只要把消息发送出去就返回成功；1表示只要Leader收到消息就返回成功；all表示所有副本都写入数据成功才算成功
        props.put("acks", PropertiesUtil.get("kafka.dau.producer.acks"))
        //重试次数
        props.put("retries", PropertiesUtil.get("kafka.dau.producer.retries"))
        //props.put("retries", Integer.MAX_VALUE)
        //批处理的字节数
        props.put("batch.size", PropertiesUtil.get("kafka.dau.producer.batch-size"))
        //批处理的延迟时间，当批次数据未满之时等待的时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.get("kafka.dau.producer.linger-ms"))
        //用来约束KafkaProducer能够使用的内存缓冲的大小的，默认值32MB
        props.put("buffer.memory", PropertiesUtil.get("kafka.dau.producer.buffer-memory"))
        // properties.put("value.serializer",
        // "org.apache.kafka.common.serialization.ByteArraySerializer");
        // properties.put("key.serializer",
        props.put("value.serializer", PropertiesUtil.get("kafka.dau.producer.value-serializer"))
        props.put("key.serializer", PropertiesUtil.get("kafka.dau.producer.key-serializer"))
        new KafkaProducer[String, String](props)
    }

    def createDStream(ssc:StreamingContext,topic:String): InputDStream[ConsumerRecord[String, String]] ={
        //获取数据的主题
        //val fromTopic = PropertiesUtil.get("kafka.dau.topic")
        //创建kafka连接参数
        val kafkaParams = Map[String,Object](
            //集群地址
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.get("kafka.bootstrap-servers"),
            //Key与VALUE的反序列化类型
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> PropertiesUtil.get("kafka.dau.consumer.key-deserializer"),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> PropertiesUtil.get("kafka.dau.consumer.value-deserializer"),
            //创建消费者组
            ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.get("kafka.dau.consumer.group-id"),
            //自动移动到最新的偏移量
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> PropertiesUtil.get("kafka.dau.consumer.auto-offset-reset"),
            //启用自动提交，将会由Kafka来维护offset【默认为true】
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> PropertiesUtil.get("kafka.dau.consumer.enable-auto-commit")
        )
        //获取DStream
        KafkaUtils.createDirectStream(
            ssc,//SparkStreaming操作对象
            LocationStrategies.PreferConsistent,//数据读取之后如何分布在各个分区上
            /*
            PreferBrokers：仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在kafka broker的机器上
            PreferConsistent：大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
            PreferFixed：如果你的负载不均衡，可以通过这种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配
             */
            ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams)
        )
    }

//    var instance: KafkaTool = _
//
//    def getInstance() = {
//        if (instance == null) instance = apply()
//        instance
//    }
//
//    def getInstance(producer: KafkaProducer[String, String], consumer: KafkaConsumer[String, String]) = new KafkaTool(producer, consumer) = {
//        if (instance == null) instance = apply(producer, consumer)
//    }

    def apply(adminClient: AdminClient, producer: KafkaProducer[String, String], consumer: KafkaConsumer[String, String]): KafkaTool = new KafkaTool(adminClient, producer, consumer)
    def apply(producer: KafkaProducer[String, String], consumer: KafkaConsumer[String, String]) = new KafkaTool(producer, consumer)
    def apply(adminClient: AdminClient) = new KafkaTool(adminClient)
    def apply() = new KafkaTool()

}

class KafkaTool (@BeanProperty var adminClient: AdminClient, private[KafkaTool] var defaultProducer: KafkaProducer[String, String], private[KafkaTool] var defaultConsumer: KafkaConsumer[String, String]) extends Serializable {

    private[KafkaTool] def this() {
        this(KafkaTool.createAdmin(), KafkaTool.createProducer, KafkaTool.createConsumer)
    }

    private[KafkaTool] def this(admin: AdminClient) {
        this(admin, KafkaTool.createProducer, KafkaTool.createConsumer)
    }

    private[KafkaTool] def this(producer: KafkaProducer[String, String], consumer: KafkaConsumer[String, String]) {
        this(KafkaTool.createAdmin(), producer, consumer)
    }

    import java.util.Arrays
    import scala.collection.JavaConversions._
    def createTopic(topicName: String, numPartitions: Int, replicationFactor: Int) ={
        val configs = new mutable.HashMap[String, String]()
        val res = adminClient.createTopics(Arrays.asList(new NewTopic(topicName, numPartitions, replicationFactor.toShort).configs(configs)))

        for (entry <- res.values().entrySet()) {
            try {
                entry.getValue.get()
                println("topic " + entry.getKey + " created")
            } catch {
                case ex: (InterruptedException, ExecutionException) => println("topic " + entry.getKey + " existed")
            }
        }
    }

    def existsTopic(topicName: String) = {
        listTopics.contains(topicName)
    }

    import scala.collection.JavaConversions.asScalaSet
    def listTopics = {
        if (this.adminClient == null) createAdmin()
        val res = adminClient.listTopics()
        res.names().get().map(_ + "\t")
        res.names().get()
    }


    def sendMessage(topic: String, jsonMessage: String): Unit = {
        val producer = KafkaTool.createProducer
        producer.send(new ProducerRecord[String, String](topic, jsonMessage))
        producer.close()
    }

    def sendMessage(topic: String, jsonMessages: String*): Unit = {
        val producer = KafkaTool.createProducer
        for (jsonMessage <- jsonMessages) {
            producer.send(new ProducerRecord[String, String](topic, jsonMessage))
        }
        producer.close()
    }

    import org.json4s._
    import org.json4s.native.Serialization._
    import org.json4s.native.Serialization
    implicit val formats = Serialization.formats(NoTypeHints)

    def sendMessage(topic: String, mapMessages: List[java.util.Map[Any, Any]]): Unit = {
        val producer = KafkaTool.createProducer
        for (mapMessage <- mapMessages) {
            val message: String = write(mapMessage)
            producer.send(new ProducerRecord[String, String](topic, message))
        }
        producer.close()
    }

    def sendMessage(topic: String, mapMessage: Map[Any, Any]): Unit = {
        val producer = KafkaTool.createProducer
        val message: String = write(mapMessage)
        producer.send(new ProducerRecord[String, String](topic, message))
        producer.close()
    }
}



























































