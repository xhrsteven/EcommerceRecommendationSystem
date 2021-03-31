
import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description: some desc
 * @author: Steven Xu
 * @email: xhrsteven@gmail.com
 * @date: 2021/3/30 17:27
 */
// 定义样例类
case class Product(productId:Int,
                   name:String,
                   imageUrl:String,
                   categories:String,
                   tags:String)

case class Rating(userId:Int,
                  productId:Int,
                  score:Double,
                  timestamp:Int)

case class MongoConfig(uri:String, db:String)

object DataLoader {

  val PRODUCT_DATA_PATH="E:\\EcommerceRecommendationSystem\\EcommerceRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH="E:\\EcommerceRecommendationSystem\\EcommerceRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION="Product"
  val MONGODB_RATING_COLLECTION="Rating"

  def storeDataInMongoDB(productDF: DataFrame, rateingDF: DataFrame)
                        (implicit mongoConfig: MongoConfig):Unit = {
    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 定义通过MongoDB客户端拿到的表操作对象
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)

    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果MongoDB中有对应的数据库，那么应该删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据写入到MongoDB
    productDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    rateingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))

    mongoClient.close()
  }

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config =Map(
      "spark.cores" -> "local[*]",
      "mongo.uri"->"mongodb://hadoop02:27017/recommender",
      "mongo.db"->"recommender"
    )
    // 创建一个SparkConf配置
    val sparkConf = new SparkConf()
      .setAppName("DataLoader")
      .setMaster(config("spark.cores"))

    // 创建一个SparkSession
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // 在对DataFrame和Dataset进行操作许多操作都需要这个包进行支持
    import spark.implicits._
    // 将Product、Rating数据集加载进来
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)

    //将ProdcutRDD装换为DataFrame
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

    val rateingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // 声明一个隐式的配置对象
    implicit  val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    storeDataInMongoDB(productDF,rateingDF)


    spark.stop()
  }


}
