package com.bigdata.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.sql.Date
import java.text.SimpleDateFormat

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/30 18:34
 */
case class Rating(userId:Int, productId:Int,score:Double,timestamp:Int)

case class MongoConfig(uri:String,db:String)

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION ="Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS ="RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS ="RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS="AverageProducts"

  // 入口方法
  def main(args: Array[String]): Unit = {
      val config = Map(
        "spark.cores" -> "local[*]",
        "mongo.uri" -> "mongodb://hadoop02:27017/recommender",
        "mongo.db"->"recommender"
      )
    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val mongoConfig =MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._

    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()


     //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO: 不同的统计推荐结果

    //统计所有历史数据中每个商品的评分数
    //数据结构 -》  productId,count
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId")

    rateMoreProductsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计以月为单位拟每个商品的评分数
    //数据结构 -》 productId,count,time
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp装换成年月格式   1260759144000  => 201605
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // 将原来的Rating数据集中的时间转换成年月的格式
    val ratingOfYearMonth = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from rating")

    // 将新的数据集注册成为一张表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProducts = spark.sql("select productId, count(productId) as count, yearmonth" +
      "from ratingOfMonth" +
      "group by productId, yearmonth" +
      "order by yearmonth desc , count desc ")

    rateMoreRecentlyProducts.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId")

    averageProductsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_PRODUCTS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()


  }

}
