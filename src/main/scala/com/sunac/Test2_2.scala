package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Test2_2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val s1: String =
      """
        |select fld_guid,fld_area_guid,fld_object_guid,fld_owner_guid,fld_is_owner
        |from(select *, row_number()over(partition by fld_area_guid,fld_object_guid,fld_owner_guid order by fld_is_current desc,fld_is_charge desc,fld_status desc) ranking
        |from test.es_info_object_and_owner) t where t.ranking = 1;
        |""".stripMargin
    spark.sql(s1).createTempView("oao")
    val s2: String =
      """
        |select
        |    f.*,
        |    oao.fld_guid as oao_fld_guid,
        |    oao.fld_is_owner
        |from test.xiyang2 f left join oao oao
        |on f.o_fld_guid=oao.fld_object_guid
        |and oao.fld_owner_guid=f.w_fld_guid
        |and oao.fld_area_guid=f.fld_area_guid
        |""".stripMargin
    val df: DataFrame = spark.sql(s2)
    df.repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.xiyang3")
    spark.stop()
  }
}
