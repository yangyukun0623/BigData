package cn.ikun.demo.controller

import cn.ikun.demo.tools.KafkaTool
import com.alibaba.fastjson.{JSON, JSONObject}
import lombok.extern.slf4j.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.{RequestBody, RequestMapping, RestController}

@RestController
@Component
class WebLoggerController {

    @Bean
    var kafkaTemplate: KafkaTool = KafkaTool.apply()

    @RequestMapping(Array("/applog"))
    def logger(@RequestBody logger: String): Unit = {
        //暂时输出到控制台
        println(logger)
        //落地文件中
        val json: JSONObject = JSON.parseObject(logger)
        if (json.getString("start") != null && json.getString("start").length > 0)
            kafkaTemplate.sendMessage("GMALL_STARTS", logger)
        else
            kafkaTemplate.sendMessage("GMALL_EVENTS", logger)

    }
}
