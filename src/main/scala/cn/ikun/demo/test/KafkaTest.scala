package cn.ikun.demo.test

import java.time.Duration
import java.util.Collections

import cn.ikun.demo.tools.KafkaTool

object KafkaTest {
    def main(args: Array[String]): Unit = {
        val kafka = KafkaTool.apply()
        //kafka.createTopic("t4", 2, 1)
        println(kafka.existsTopic("t4"))
        println(kafka.listTopics)
        val consumer = KafkaTool.createConsumer
        //订阅主题
        consumer.subscribe(Collections.singletonList("starts"))
        import scala.collection.JavaConversions._
        for (topic <- kafka.listTopics) {
            val records = consumer.poll(Duration.ofMillis(1000))
            for (record <- records) {
                println(record.value)
            }
        }
    }
}
