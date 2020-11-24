package cn.ikun.demo

import cn.ikun.demo.app.DauApp
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class ProgramApplication

object ProgramApplication {
    def main(args: Array[String]): Unit = {
        SpringApplication.run(classOf[ProgramApplication])
//        val app = context.getBean(classOf[DauApp])
//        app.run()
    }
}
