import org.apache.spark.sql
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StringType}
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession, TypedColumn}
import org.apache.spark.sql.functions.{lit, udf, when, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object third {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkPOC")
      .getOrCreate()

    val data = Seq(("Raj", "O", "V", "25", "M", 3000),
      ("Akshay", "xyz", "", "40", "", 4000),
      ("Paresh", "", "W", "424", "M", 4000),
      ("Avi", "", "", "26", "F", 4000),
      ("Shrutika", "Aasdf", "P", "23", "F", -1),
      ("Mike", "fhjjh", "B", "", "O", 0),
      ("Rosario", "Aasdf", "Barluskony", "", "P", 6535)
    )

    val columns = Seq("firstname", "middlename", "lastname", "age", "gender", "salary")
    import spark.sqlContext.implicits._
    var df = data.toDF(columns: _*)

    var Df1 = df.withColumn("ErrorCode", lit(null).cast(StringType))
    //var DF2 = Df1.select(df("firstname").cast(DataTypes.StringType).as("First Name"), df("salary").cast(DataTypes.IntegerType).as("Salary"), $"ErrorCode").show()
    Df1.foreach(row => {
     // println(" hsghdsgdfjjhj " + row.getAs(0).toString())
      var firstName=row.getAs("firstname").toString
      var middleName=row.getAs("middlename").toString
      var lastName=row.getAs("lastname").toString
      var age= toInt(row.getAs("age").toString)
      var gender=row.getAs("gender").toString
      var salary= toInt(row.getAs("salary").toString)
      //var sal:BigDecimal=row.getDecimal(5)

  println(firstName.toString +lastName.toString+age.toString+gender.toString +salary)
    })
  }

  def toInt(in: String): Option[Int] = {
    try {
      Some(Integer.parseInt(in.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }

  /*
      * E01-> Null Value
      * E02 -> Empty Value
      * E03 --> Age not in Range[0-125]
      * E04 --> Invalid Gender[M,F,O]
      * E05 ---> Zero or Negative Salary
      * E06,E06 ---> Dummy Errors
       */
  def validateAge(age: Int):Option[String] = {
    if(age==null)
      return Some("EO1")
    else if(age >= 0 && age < 125)
      return None
    else
      return Some("EO3")
  }

  def validateNames(name: String):Option[String] = {
    if(name==null)
      return Some("EO1")
    else if (name.equals(""))
      return Some("EO2")
    else
      return None
  }

  def validateSalary(salary: BigDecimal):Option[String] = {
    if(salary==null)
      return Some("EO1")
    else if(salary <= 0)
      return Some("E05")
    else
      return None
  }
  def validateGender(gender: String):Option[String] = {
    val genderList = List("M", "F", "O")
    if(gender==null)
      return Some("EO1")
    else if (gender.equals(""))
      return Some("EO2")
    else if (genderList.contains(gender))
      return None
    else
    return Some("EO4")
  }
}