package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ToTiDBTask {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    spark.sql(SqlConfig.INSERT_ALL_SQL_2).repartitionByRange(1000, col("fld_guid"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .option("truncate", "true")
      .option("batchsize", 2000)
      .option("isolationLevel", "NONE")
      .jdbc(SqlConfig.JDBC_STR_2, "es_charge_owner_fee_all_data", SqlConfig.prop)
    spark.stop()
  }
}
