package cn.ikun.demo.entry

import org.springframework.data.elasticsearch.annotations.Document

/**
 * @author zhang
 * @date 2020/11/1 22:51
 */
@Document(indexName="dau_list_info")
case class Dau(mid:String,//设备号
               uid:String,//用户ID
               ar:String,//区域
               ch:String,//渠道
               vc:String,//程序版本号
               dt:String,//日期
               hr:String,//小时
               ts:Long//时间戳
              ) extends Serializable {
}
