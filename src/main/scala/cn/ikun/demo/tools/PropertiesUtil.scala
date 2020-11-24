package cn.ikun.demo.tools

import java.util.Properties

/** 获取配置信息，默认加载类路径下的application.properties配置文件
 * @author zhang
 * @date 2020/10/23 15:56
 */
object PropertiesUtil {
    val prop = new Properties
    prop.load(this.getClass.getClassLoader.getResourceAsStream("application.properties"))

    /**
     * 加载配置文件，获取配置信息
     * @param filepath 配置文件在类路径中的相对路径
     * @return 返回Properties对象
     */
    def load(filepath:String): Properties = {
        val prop = new Properties
        prop.load(this.getClass.getClassLoader.getResourceAsStream(filepath))
        prop
    }

    /**
     * 根据key获取此配置的值
     * @param key 配置文件中的key
     * @return 此key对应的value
     */
    def get(key:String): String ={
        prop.getProperty(key)
    }
    /**
     * 测试
     * @param args 参数
     */
    def main(args: Array[String]): Unit = {
        println(prop.getProperty("aaa"))
        println(prop.getProperty("a"))
    }
}
/** 获取配置信息，自定义加载类路径下的application.properties配置文件，得到的是Properties对象
 * @author zhang
 * @date 2020/10/23 15:56
 */
class PropertiesUtil(val filepath:String){
    val prop = new Properties
    prop.load(this.getClass.getClassLoader.getResourceAsStream(filepath))
    prop
}
