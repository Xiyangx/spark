import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

/**
 * @author litten
 * @date 2022/6/10 10:54
 */
object Config {
  val dropSql = "alter table sunac.ads_label_person_second_period_md_df drop if exists partition(day='%s',type='%s')"
  val insertHeadSql =
    """
      | INSERT OVERWRITE TABLE sunac.ads_label_person_second_period_md_df partition(day,type)
      |""".stripMargin

  def insertUtil(spark: SparkSession, dayPart: String, typePart: String, insertSql: String, isTest: Boolean = false) = {
    if (isTest) {
      spark.sql(insertSql.format(dayPart, typePart)).show(100, false)
    } else {
      spark.sql(dropSql.format(dayPart, typePart))
      //    println(dropSql.format(dayPart, typePart))
      //    println(insertHeadSql + insertSql.format(dayPart, typePart))
      spark.sql(insertHeadSql + insertSql.format(dayPart, typePart))
    }
  }

  val phoneFun = (home_phone: String, ec_phone: String, life_phone: String, gx_register_phone: String) => {
    import scala.collection.mutable.Set
    val set = Set[String](home_phone, ec_phone, life_phone, gx_register_phone)
    set.filter(_ != null).toList
  }

  val estateFun = (housing_estate: mutable.WrappedArray[String]) => {
    var s: mutable.WrappedArray[String] = housing_estate
    // 如果size > 1，删除 "虚拟小区"
    if (housing_estate.size > 1) {
      s = housing_estate.filter(_ != "虚拟小区")
    }
    s
  }
}
