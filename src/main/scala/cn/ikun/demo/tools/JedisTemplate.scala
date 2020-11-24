package cn.ikun.demo.tools


import java.lang
import java.time.Duration

import com.alibaba.fastjson.{JSON, JSONObject}
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.serializer.{JdkSerializationRedisSerializer, RedisSerializer}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, ListPosition}

import scala.collection.JavaConversions._
import scala.collection.mutable

object JedisTemplate extends JdkSerializationRedisSerializer with Serializable {
    private def defaultJedisPoolConfig(): JedisPoolConfig = {
        val conf = new JedisPoolConfig
        conf.setMaxIdle(PropertiesUtil.get("dau.redis.jedis.pool.max-idle").toInt)
        conf.setMinIdle(PropertiesUtil.get("dau.redis.jedis.pool.min-idle").toInt)
        conf.setMaxWaitMillis(PropertiesUtil.get("dau.redis.jedis.pool.max-wait").toInt)
        conf.setMaxTotal(PropertiesUtil.get("dau.redis.jedis.pool.max-active").toInt)
        //忙碌时是否等待
        conf.setBlockWhenExhausted(true)
        //每次获得连接进行连接的测试
        conf.setTestOnBorrow(true)
        conf
    }
}

class JedisTemplate (private var conf: JedisPoolConfig, private var host: String, private var port: Int) {
    private var jedisPool: JedisPool = _
    private var jedis: Jedis = _

    def this() {
        this(JedisTemplate.defaultJedisPoolConfig(), "127.0.0.1", 6379)
        this.jedisPool = new JedisPool(conf, host, port)
        this.jedis = jedisPool.getResource
    }

    def this(jedisPoolConfig: JedisPoolConfig) {
        this(jedisPoolConfig, "127.0.0.1", 6379)
        this.jedisPool = new JedisPool(conf, host, port)
        this.jedis = jedisPool.getResource
    }

    def this(host: String, port: Int) = {
        this(JedisTemplate.defaultJedisPoolConfig(), host, port)
        this.jedisPool = new JedisPool(conf, host, port)
        this.jedis = jedisPool.getResource
    }

    def opsForValue = new ValueOperations
    def opsForList = new ListOperations
    def opsForHash = new HashOperations
    def opsForSet = new SetOperations
    def opsForZSet = new ZSetOperations

    private[JedisTemplate] class KeyOperations extends JdkSerializationRedisSerializer{
        def ping: String = jedis.ping()
        def ping(message: String): String = jedis.ping(message)
        def exists(key: String): Boolean = jedis.exists(key)
        def exists(keys: String*): Boolean = {
            var isExists = true
            for (key <- keys) {
                isExists = isExists && this.exists(key)
            }
            isExists
        }
        def keys(pattern: String): mutable.Set[String] = {
            val set = new mutable.HashSet[String]()
            val keys = jedis.keys(pattern)
            for (key <- keys) {
                set.add(key)
            }
            set
        }
        def keys(): mutable.Set[String] = this.keys("*")
        def delete(key: String): Long = jedis.del(key)
        def delete(keys: String*): Long = {
            var res: Long = 0
            for (key <- keys) {
                res = this.delete(key)
            }
            res
        }
        //设置键过期时间
        def expire(key: String, seconds: Int): Long = jedis.expire(key, seconds)
        //
        def pexpire(key: String, milliseconds: Long): Long = jedis.pexpire(key, milliseconds)
        //移除键的时间限制
        def persist(key: String): Long = jedis.persist(key)
        //获取键剩余时间
        def ttl(key: String): Long = jedis.ttl(key)
        //查看key所对应value的数据类型
        def valueType(key: String): String = jedis.`type`(key)

    }

    class ValueOperations extends KeyOperations with Serializable {
        def get(key: String): String = jedis.get(key)
        def get(key: String, start: Long, end: Long): String = jedis.getrange(key, start, `end`)
        def multiGet(keys: String*): List[String] = {
            val list = List[String]()
            keys.foreach(list.+(_))
            list
        }

        def set(key: String, value: String): String = jedis.set(key, value)
        //不覆盖增加数据项（重复的不插入）
        def setnx(key: String, value: String): Long = jedis.setnx(key, value)
        //增加数据项并设置有效时间
        def set(key: String, value: String, timeout: Int): String = jedis.setex(key, timeout, value)
        //向已有的键值追加一个字符串
        def append(key: String, value: String): Long = jedis.append(key, value)
        //获取key对应的value并更新value
        def getAndSet(key: String, value: String): String = jedis.getSet(key, value)

        //删除键为key的数据项
        override def delete(key: String): Long = jedis.del(key)
        //删除多个key对应的数据项
        override def delete(keys: String*): Long = {
            var res: Long = 0
            for (key <- keys) {
                res = jedis.del(key)
            }
            res
        }

        //对数字符串的操作
        //键值自增1
        def increment(key: String): Long = jedis.incr(key)
        //键值增加指定数
        def increment(key: String, delta: Long): Long = jedis.incrBy(key, delta)
        //键值自减1
        def decrement(key: String): Long = jedis.decr(key)
        //键值减少指定数量
        def decrement(key: String, delta: Long): Long = jedis.decrBy(key, delta)
        def size(key: String): Int = jedis.keys(key).size()
    }

    class ListOperations extends KeyOperations with Serializable {
        //获取列表指定下标的元素
        def index(key: String, index: Long): String = jedis.lindex(key, index)
        //列表左边元素弹出
        def leftPop(key: String): String = jedis.lpop(key)
        //列表左边添加元素
        def leftPush(key: String, value: String): Long = jedis.lpush(key, value)
        def leftPush(key: String, pivot: String, value: String): Long = jedis.linsert(key, ListPosition.BEFORE, pivot, value)
        //列表批量添加元素
        def leftPushAll(key: String, values: Seq[String]): Long = {
            var res: Long = 0
            for (value <- values) {
                res = jedis.lpush(key, value)
            }
            res
        }
        def leftPushAll(key: String, values: String*): lang.Long = {
            var res: Long = 0
            for (value <- values) {
                res = jedis.lpush(key, value)
            }
            res
        }

//        def range(key: String, start: Long, end: Long) = {}
        def rightPop(key: String): String = jedis.rpop(key)
        def rightPush(key: String, value: String): Long = jedis.rpush(key, value)
//        def rightPush(key: String, pivot: String, value: String) = {}
//        def rightPushAll(key: String, values: Seq[String]) = {}
//        def rightPushAll(key: String, values: String*) = {}
//        def trim(key: String, start: Long, end: Long) = {}
        def size(key: String): Long = jedis.llen(key)
//        def set(key: String, index: Long, value: String) = {}

        def sort(key: String): List[String] = jedis.sort(key).toList
    }
    class HashOperations extends KeyOperations with Serializable {}
    class SetOperations extends KeyOperations with Serializable {
        import scala.collection.JavaConversions._
        def add(key: String, values: String*): Unit = {
            for (value <- values) {
                jedis.sadd(key, value)
            }
        }
        def add(key: String, value: String) = jedis.sadd(key, value)

        def add(key: String, value: String, duration: Long) = {
            if(opsForSet.exists(key)){
                jedis.sadd(key, Array[String](value):_*)
            }else{
                jedis.sadd(key, Array[String](value):_*)
                jedis.expire(key, duration.toInt)
            }

        }
        //获取key对应的Set
        def members(key: String) = jedis.smembers(key)
    }
    class ZSetOperations extends KeyOperations with Serializable {}

}




























































