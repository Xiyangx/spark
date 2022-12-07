package com.sunac

import com.sunac.Test2.{JdbcUtil, JdbcUtilBySlide, JdbcUtilBySlide2}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TableMoveTask {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig.initSpark(this.getClass.getSimpleName.stripSuffix("$"))

    //    SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_owner_fee", "es_charge_owner_fee"), "es_charge_owner_fee")
    //    SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_incoming_fee", "es_charge_incoming_fee"), "es_charge_incoming_fee")
    //    SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_object", "es_info_object"), "es_info_object")
    if (args(0) == "1"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_owner", "es_info_owner"), "es_info_owner")
    }
    if (args(0) == "2"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_param_info", "es_info_param_info"), "es_info_param_info")
    }
    if (args(0) == "3"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_area_info", "es_info_area_info"), "es_info_area_info")
    }
    if (args(0) == "4"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_project", "es_charge_project"), "es_charge_project")
    }
    if (args(0) == "5"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_rate_result", "es_charge_rate_result"), "es_charge_rate_result")
    }
    if (args(0) == "6"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_object_class", "es_info_object_class"), "es_info_object_class")
    }
    if (args(0) == "7"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_object_and_owner", "es_info_object_and_owner"), "es_info_object_and_owner")
    }
    if (args(0) == "8"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_project_period_join", "es_charge_project_period_join"), "es_charge_project_period_join")
    }
    if (args(0) == "9"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_project_period", "es_charge_project_period"), "es_charge_project_period")
    }
    if (args(0) == "10"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_settle_accounts_detail", "es_charge_settle_accounts_detail"), "es_charge_settle_accounts_detail")
    }
    if (args(0) == "11"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_settle_accounts_main", "es_charge_settle_accounts_main"), "es_charge_settle_accounts_main")
    }
    if (args(0) == "12"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_ticket_pay_detail", "es_charge_ticket_pay_detail"), "es_charge_ticket_pay_detail")
    }
    if (args(0) == "13"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_ticket_pay_operate", "es_charge_ticket_pay_operate"), "es_charge_ticket_pay_operate")
    }
    if (args(0) == "14"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_commerce_bond_main", "es_commerce_bond_main"), "es_commerce_bond_maine")
    }
    if (args(0) == "15"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_commerce_bond_fee_object", "es_commerce_bond_fee_object"), "es_commerce_bond_fee_object")
    }
    if (args(0) == "16"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_incoming_fee", "es_charge_incoming_fee"), "es_charge_incoming_fee")
    }
    if (args(0) == "17"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_incoming_data", "es_charge_incoming_data"), "es_charge_incoming_data")
    }
    if (args(0) == "18"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_pay_mode", "es_charge_pay_mode"), "es_charge_pay_mode")
    }
    if (args(0) == "19"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_object_park", "es_info_object_park"), "es_info_object_park")
    }
    if (args(0) == "20"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_incoming_data_bill", "es_charge_incoming_data_bill"), "es_charge_incoming_data_bill")
    }
    if (args(0) == "21"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_bill", "es_charge_bill"), "es_charge_bill")
    }
    if (args(0) == "22"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_bill_type", "es_charge_bill_type"), "es_charge_bill_type")
    }
    if (args(0) == "23"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_two_balance", "es_charge_two_balance"), "es_charge_two_balance")
    }
    if (args(0) == "24"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_hand_in_record", "es_charge_hand_in_record"), "es_charge_hand_in_record")
    }
    if (args(0) == "25"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_voucher_check_service_set", "es_charge_voucher_check_service_set"), "es_charge_voucher_check_service_set")
    }
    if (args(0) == "26"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_voucher_project_pay", "es_charge_voucher_project_pay"), "es_charge_voucher_project_pay")
    }
    if (args(0) == "27"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_incoming_back", "es_charge_incoming_back"), "es_charge_incoming_back")
    }
    if (args(0) == "28"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_voucher_mast_refund", "es_charge_voucher_mast_refund"), "es_charge_voucher_mast_refund")
    }
    if (args(0) == "29"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_charge_owner_fee", "es_charge_owner_fee"), "es_charge_owner_fee")
    }
    if (args(0) == "30"){
      SqlConfig.insertHiveUtil(JdbcUtilBySlide(spark, "es_info_object", "es_info_object"), "es_info_object")
    }

//    if (args(0) == "16"){
//      val Slide_FEE1_Sql: String = SqlConfig.SLIDE_SQL.format("fld_area_guid", "es_charge_incoming_fee", "fld_area_guid")
//      SqlConfig.insertHiveUtil(JdbcUtilBySlide2(spark, SqlConfig.ES_CHARGE_INCOMING_FEE1, Slide_FEE1_Sql, "fld_area_guid"), "es_charge_incoming_fee")
//    }
//    if (args(0) == "17"){
//      val Slide_DA_Sql: String = SqlConfig.SLIDE_SQL.format("fld_area_guid", "es_charge_incoming_data", "fld_area_guid")
//      SqlConfig.insertHiveUtil(JdbcUtilBySlide2(spark, SqlConfig.ES_CHARGE_INCOMING_DATA, Slide_DA_Sql, "fld_area_guid"), "es_charge_incoming_data")
//    }

    spark.stop()
  }
}
