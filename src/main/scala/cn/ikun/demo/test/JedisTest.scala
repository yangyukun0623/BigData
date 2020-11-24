package cn.ikun.demo.test

import cn.ikun.demo.tools.JedisTemplate

object JedisTest {
    def main(args: Array[String]): Unit = {
        val jedisTemplate = new JedisTemplate()
        println(jedisTemplate.opsForValue.ping)
        jedisTemplate.opsForValue.set("k1", "v1")
        println(jedisTemplate.opsForValue.keys("*"))
    }
}
