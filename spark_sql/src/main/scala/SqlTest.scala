import org.apache.spark
import org.apache.spark.sql.SparkSession

object SqlTest {

    case class Person(name: String, age: Int, gender: String)

    def plus(x: Int)(y: Int) : Int = {
        val z: Int = x + y
        z
    }

    def minus(x : Int, y: Int) : Int = plus(x)(y)

    def bigger(o: Any): Any = {
        o match {
            case i: Int if i < 0 => i - 1
            case i: Int => i + 1
            case d: Double if d < 0.0 => d - 0.1
            case d: Double => d + 0.1
            case text: String => text + "s"
        }
    }

    def main(args: Array[String]) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\zhangyazhong\\IdeaProjects\\spark_demo\\hadoop")

        val sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("SparkDemo")
            // Windows extra setting
            .config("spark.sql.warehouse.dir", "file:///")
            .getOrCreate()

        /*
        val df = sparkSession.read.json("spark_sql\\src\\main\\resources\\persons.json")
        df.createOrReplaceTempView("person")
        val sqlDF = sparkSession.sql("SELECT * FROM person WHERE age<20")
        sqlDF.show()
        */

        import sparkSession.implicits._
        val personDataFrame = sparkSession.sparkContext.textFile("spark_sql\\src\\main\\resources\\persons.txt")
            .map(_.split(","))
            .map(cols => Person(cols(0), cols(1).trim.toInt, cols(2)))
            .toDF()

        personDataFrame.createOrReplaceTempView("person")
        val teenagersDF = sparkSession.sql("SELECT name, age, gender FROM person WHERE age BETWEEN 13 AND 19")
        teenagersDF.show()
    }
}
