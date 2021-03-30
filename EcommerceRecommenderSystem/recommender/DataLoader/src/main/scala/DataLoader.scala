import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

  val PRODUCT_DATA_PATH="E:\\EcommerceRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH="E:\\EcommerceRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION="Product"
  val MONGODB_RATING_COLLECTION="Rating"
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


  }


}
