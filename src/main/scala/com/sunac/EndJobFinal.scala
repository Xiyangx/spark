package com.sunac

import com.sunac.StateJobRaw.{insertUtilPartition, truncateTanleUtil}
import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/*CREATE TABLE `idx_es_charge_bill_type` (
`fld_guid` varchar(200) COLLATE utf8mb4_general_ci NOT NULL COMMENT '主键',
`key` varchar(200) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '创建人',
PRIMARY KEY (`fld_guid`),
KEY `idx_key` (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


drop table idx_es_charge_bill_type

truncate table idx_es_charge_bill_type*/


object EndJobFinal {
  def concatUtil(col: String): String = {
    "select fld_guid,%s as key from all_tab".format(col)
  }

  def insertUtil(df: DataFrame, tab: String): Unit = {
    df.repartitionByRange(50, col("fld_guid"), rand)
      .write
      .mode(SaveMode.Overwrite)
      .option("truncate", "true")
      .option("batchsize", 5000)
      .option("isolationLevel", "NONE")
      .jdbc(SqlConfig_1.JDBC_STR_2, tab, SqlConfig_1.prop)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SqlConfig_1.initSpark(this.getClass.getSimpleName.stripSuffix("$"))
    val jobName: String = args(0)
    jobName match {
      case "es_charge_incoming_fee" => Job1(spark)


      case "es_info_area_info" => Job2(spark)
      case "es_info_object" => Job3(spark)
      case "es_info_owner" => Job4(spark)
      case "es_charge_project" => Job5(spark)
      case "es_charge_pay_mode" => Job6(spark)



      case "es_charge_owner_fee" => Job7(spark)
      case "es_info_object_park" => Job8(spark)
      case "es_info_object_class" => Job9(spark)
      case "es_charge_settle_accounts_detail" => Job11(spark)
      case "es_charge_two_balance" => Job12(spark)


      case "es_charge_bill_type" => Job13(spark)

      case "es_charge_settle_accounts_main" => Job14(spark)
      case "es_charge_project_period_join" => Job15(spark)
      case "es_charge_project_period" => Job16(spark)
      case "es_charge_hand_in_record" => Job17(spark)
      case "es_charge_incoming_fee1" => Job18(spark)
      case "es_charge_voucher_check_service_set" => Job19(spark)
      case "es_charge_voucher_project_pay" => Job20(spark)

      case "es_info_object_and_owner" => Job10(spark)
      case "idx_es_charge_incoming_back" => Job21_1(spark)
      case "idx_es_charge_incoming_convert" => Job21_2(spark)
      case "idx_es_charge_incoming_kou" => Job21_3(spark)
      case "es_charge_voucher_mast_refund_back" => Job22(spark)

      case "es_charge_voucher_mast_refund_convert" => Job23(spark)
      case "es_charge_voucher_mast_refund_kou" => Job24(spark)

    }
    spark.stop()
  }

  def Job1(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_incoming_fee")
    val insertSql =
      """
        |select
        | fld_guid,
        | md5(concat_ws('',fld_area_guid,fld_incoming_fee_guid)) as fld_value
        | from test.all_tab
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_incoming_fee")
    //1,ods_fd_es_charge_incoming_fee_af f ON f.fld_guid = d.fld_incoming_fee_guid
//    insertUtil(spark.sql(concatUtil("fld_incoming_fee_guid")), "idx_es_charge_incoming_fee")
  }

  def Job2(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 2,LEFT JOIN ods_fd_es_info_area_info_af a ON a.fld_guid = d.fld_area_guid
    insertUtil(spark.sql(concatUtil("fld_area_guid")), "idx_es_info_area_info")
  }

  def Job3(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_object_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 3,ods_fd_es_info_object_af o ON o.fld_guid = d.fld_object_guid
    insertUtil(spark.sql(concatUtil("fld_object_guid")), "idx_es_info_object")
  }

  def Job4(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_owner_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 4,ods_fd_es_info_owner_af w ON w.fld_guid = d.fld_owner_guid
    insertUtil(spark.sql(concatUtil("fld_owner_guid")), "idx_es_info_owner")
  }


  def Job5(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_project_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 5,ods_fd_es_charge_project_af p ON p.fld_guid = d.fld_project_guid
    insertUtil(spark.sql(concatUtil("fld_project_guid")), "idx_es_charge_project")
  }

  def Job6(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_pay_mode_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 6,ods_fd_es_charge_pay_mode_af m ON m.fld_guid = d.fld_pay_mode_guid
    insertUtil(spark.sql(concatUtil("fld_pay_mode_guid")), "idx_es_charge_pay_mode")
  }

  def Job7(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_owner_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 7,ods_fd_es_charge_owner_fee_af cof ON cof.fld_guid = d.fld_owner_fee_guid AND cof.fld_area_guid = d.fld_area_guid
    insertUtil(spark.sql(concatUtil("md5(concat_ws('',fld_owner_fee_guid,fld_area_guid)) ")), "idx_es_charge_owner_fee")
  }

  def Job8(spark: SparkSession): Unit = {
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_object_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    // 8, ods_fd_es_info_object_park_af op ON op.fld_guid = d.fld_object_guid AND op.fld_area_guid = d.fld_area_guid
    insertUtil(spark.sql(concatUtil("md5(concat_ws('',fld_object_guid,fld_area_guid)) ")), "idx_es_info_object_park")

  }

  def Job9(spark: SparkSession): Unit = {
    // 9,ods_fd_es_info_object_class_af oc ON oc.fld_guid = o.fld_class_guid
    val mainStr2 =
      """
        |select
        | t1.fld_guid,
        | t2.fld_class_guid fld_class_guid
        | from t1 join t2 on t1.fld_object_guid=t2.fld_guid
        |""".stripMargin
    spark.sql("select fld_area_guid,fld_guid,fld_object_guid from test.all_tab").createTempView("t1")
    spark.sql("select fld_guid,fld_class_guid,fld_area_guid from test.object").createTempView("t2")
    spark.sql(mainStr2).createTempView("all_tab")
    insertUtil(spark.sql(concatUtil("fld_class_guid")), "idx_es_info_object_class")
  }

  def Job10(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_info_object_and_owner")
    // 10,--
    //LEFT JOIN ods_fd_es_info_object_and_owner_af ao ON
    // d.fld_area_guid
    // d.fld_object_guid
    // d.fld_owner_guid
    val mainStr =
    """
      |select
      | fld_guid,
      | fld_area_guid,
      | fld_object_guid,
      | fld_owner_guid
      | from test.all_tab
      |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,md5(concat_ws('',fld_area_guid,fld_object_guid,fld_owner_guid)) as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_info_object_and_owner")
//    insertUtil(spark.sql(concatUtil("md5(concat_ws('',fld_area_guid,fld_object_guid,fld_owner_guid)) ")), "idx_es_info_object_and_owner")
  }

  def Job11(spark: SparkSession): Unit = {
    // 11,ods_fd_es_charge_settle_accounts_detail_af ad
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_owner_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    insertUtil(spark.sql(concatUtil("md5(concat_ws('',fld_owner_fee_guid,fld_area_guid)) ")), "idx_es_charge_settle_accounts_detail")

  }

  def Job12(spark: SparkSession): Unit = {
    // 12,   LEFT JOIN es_charge_two_balance two on two.fld_guid=d.fld_incoming_fee_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("all_tab")
    insertUtil(spark.sql(concatUtil("fld_incoming_fee_guid")), "idx_es_charge_two_balance")
  }

  def Job13(spark: SparkSession): Unit = {

    spark.sql(SqlConfig_1.ES_CHARGE_INCOMING_DATA_BILL).createTempView("table_bill")
    spark.sql(SqlConfig_1.ES_CHARGE_BILL).createTempView("table_b")

    val s1: String =
      """
        |select
        |   m.fld_data_src_guid,
        |   m.fld_type_guid
        |from
        |   (
        |       select t.*,
        |             row_number() over (partition by t.fld_data_src_guid order by fld_operate_date desc)row
        |        from
        |             (select
        |                bill.fld_data_src_guid,
        |                b.fld_type_guid,
        |                b.fld_operate_date
        |             from (select fld_data_src_guid,fld_bill_guid from table_bill where fld_data_src_guid IS NOT NULL and fld_bill_guid IS NOT NULL
        |                   )bill
        |                     join  (select fld_guid,fld_type_guid,fld_operate_date from table_b
        |                   ) b
        |             on bill.fld_bill_guid=b.fld_guid
        |             )t
        |   )m where m.row=1
        |""".stripMargin
    spark.sql(s1).createTempView("t1")
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("t2")
    val s =
      """
        |select t2.fld_guid,t1.fld_type_guid
        | from t2 join t1 on t2.fld_guid=t1.fld_data_src_guid
        |""".stripMargin
    spark.sql(s).createTempView("all_tab")
    insertUtil(spark.sql(concatUtil("fld_type_guid")), "idx_es_charge_bill_type")
  }

  def Job14(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_settle_accounts_main")
    val mainStr =
      """
        |select
        | fld_guid,fld_area_guid,fld_owner_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_SETTLE_ACCOUNTS_DETAIL: String =
      """
        |select fld_guid,fld_owner_fee_guid,fld_adjust_guid,fld_area_guid,fld_main_guid,
        |substr(fld_create_date,1,19) fld_create_date FROM test.detail
        | WHERE fld_status=1 and fld_area_guid is not null
        |""".stripMargin
    spark.sql(ES_CHARGE_SETTLE_ACCOUNTS_DETAIL).createTempView("de_1")
    spark.sql(mainStr).createTempView("m")
    val s4 =
      """
        |select m.fld_guid,m.fld_area_guid,
        |if(t1.fld_guid is null,t2.fld_guid,t1.fld_guid) ad_fld_guid,
        |if(t1.fld_main_guid is null,t2.fld_main_guid,t1.fld_main_guid) fld_main_guid,
        |if(t1.fld_create_date is null,t2.fld_create_date,t1.fld_create_date) ad_fld_create_date
        | from m
        | left join de_1 t1 on m.fld_owner_fee_guid= t1.fld_owner_fee_guid and m.fld_area_guid=t1.fld_area_guid
        | left join de_1 t2 on m.fld_owner_fee_guid= t2.fld_adjust_guid and m.fld_area_guid=t2.fld_area_guid
        |""".stripMargin
    spark.sql(s4).createTempView("de")
    val s5 =
      """
        |select fld_guid,fld_main_guid,fld_area_guid from
        |(
        | select *,row_number() over (partition by fld_guid order by ad_fld_create_date desc)row
        |    from de where ad_fld_guid is not null
        |)m where m.row=1
        |""".stripMargin
    spark.sql(s5).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,md5(concat_ws('',fld_main_guid,fld_area_guid)) as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_settle_accounts_main")
    //insertUtil(spark.sql(concatUtil("md5(concat_ws('',fld_main_guid,fld_area_guid)) ")), "idx_es_charge_settle_accounts_main")
  }

  def Job15(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_project_period_join")
    // 15,LEFT JOIN  es_charge_project_period_join pj on pj.fld_project_guid=d.fld_project_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_project_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_project_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_project_period_join")
    //insertUtil(spark.sql(concatUtil("fld_project_guid")), "idx_es_charge_project_period_join")
  }


  def Job16(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_project_period")
    // 16,es_charge_project_period pp on pj.fld_period_guid=pp.fld_guid and pp.fld_type=1
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_project_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql("SELECT fld_project_guid,fld_period_guid FROM test.project_join").createTempView("t2")
    val s =
      """
        |select t1.fld_guid,t2.fld_period_guid
        |from t1 join t2 on t1.fld_project_guid=t2.fld_project_guid
        |""".stripMargin
    spark.sql(s).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_period_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_project_period")
//    insertUtil(spark.sql(concatUtil("fld_period_guid")), "idx_es_charge_project_period")
  }

  def Job17(spark: SparkSession): Unit = {
    // LEFT JOIN es_charge_two_balance two on two.fld_guid=d.fld_incoming_fee_guid
    // LEFT JOIN es_charge_hand_in_record hir on hir.fld_guid=two.fld_hand_in_guid
    val mainStr =
    """
      |select
      | fld_guid,
      | fld_area_guid,
      | fld_incoming_fee_guid
      | from test.all_tab
      |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql("SELECT fld_guid,fld_hand_in_guid FROM test.two").createTempView("t2")
    val s =
      """
        |select t1.fld_guid,t2.fld_hand_in_guid
        |from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_guid
        |""".stripMargin
    spark.sql(s).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_hand_in_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_hand_in_record")
//    insertUtil(spark.sql(concatUtil("fld_hand_in_guid")), "idx_es_charge_hand_in_record")
  }


  def Job18(spark: SparkSession): Unit = {
    // // 18 ods_fd_es_charge_incoming_fee_af
    truncateTanleUtil("idx_es_charge_incoming_fee1")
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_area_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_incoming_fee1")
//    insertUtil(spark.sql(concatUtil("fld_area_guid")), "idx_es_charge_incoming_fee1")
  }

  def Job19(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_voucher_check_service_set")
    //LEFT JOIN es_charge_voucher_check_service_set ss
    //on ss.fld_project_class_guid=d.fld_project_guid
    val mainStr =
    """
      |select
      | fld_guid,
      | fld_area_guid,fld_project_guid
      | from test.all_tab
      |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_project_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_voucher_check_service_set")
//    insertUtil(spark.sql(concatUtil("fld_project_guid")), "idx_es_charge_voucher_check_service_set")
  }


  def Job20(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_voucher_project_pay")
    //    LEFT JOIN es_charge_voucher_project_pay vpp on vpp
    //    .fld_pay_mode_guid = d.fld_pay_mode_guid
    val mainStr =
    """
      |select
      | fld_guid,
      | fld_area_guid,fld_pay_mode_guid
      | from test.all_tab
      |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_pay_mode_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_voucher_project_pay")
//    insertUtil(spark.sql(concatUtil("fld_pay_mode_guid")), "idx_es_charge_voucher_project_pay")
  }

  def Job21_1(spark: SparkSession): Unit = {
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    truncateTanleUtil("idx_es_charge_incoming_back")
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_incoming_fee_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_incoming_back")
    //    val Slide_BACK_Sql: String = SqlConfig_1.SLIDE_SQL.format("fld_area_guid", "es_charge_incoming_back", "fld_area_guid")
    //    JdbcUtilBySlide2(spark, SqlConfig_1.ES_CHARGE_INCOMING_BACK, Slide_BACK_Sql, "fld_area_guid").createTempView("t2")
//    val df: DataFrame = spark.sql(concatUtil("fld_incoming_fee_guid")).persist()
//    insertUtil(df, "idx_es_charge_incoming_back")
//    insertUtil(df, "idx_es_charge_incoming_convert")
//    insertUtil(df, "idx_es_charge_incoming_kou")
//    df.unpersist(false)
  }
  def Job21_2(spark: SparkSession): Unit = {
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    truncateTanleUtil("idx_es_charge_incoming_convert")
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_incoming_fee_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_incoming_convert")
    //    val Slide_BACK_Sql: String = SqlConfig_1.SLIDE_SQL.format("fld_area_guid", "es_charge_incoming_back", "fld_area_guid")
    //    JdbcUtilBySlide2(spark, SqlConfig_1.ES_CHARGE_INCOMING_BACK, Slide_BACK_Sql, "fld_area_guid").createTempView("t2")
    //    val df: DataFrame = spark.sql(concatUtil("fld_incoming_fee_guid")).persist()
    //    insertUtil(df, "idx_es_charge_incoming_back")
    //    insertUtil(df, "idx_es_charge_incoming_convert")
    //    insertUtil(df, "idx_es_charge_incoming_kou")
    //    df.unpersist(false)
  }
  def Job21_3(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_incoming_kou")
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,
        | fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    spark.sql(mainStr).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,fld_incoming_fee_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_incoming_kou")
    //    val Slide_BACK_Sql: String = SqlConfig_1.SLIDE_SQL.format("fld_area_guid", "es_charge_incoming_back", "fld_area_guid")
    //    JdbcUtilBySlide2(spark, SqlConfig_1.ES_CHARGE_INCOMING_BACK, Slide_BACK_Sql, "fld_area_guid").createTempView("t2")
    //    val df: DataFrame = spark.sql(concatUtil("fld_incoming_fee_guid")).persist()
    //    insertUtil(df, "idx_es_charge_incoming_back")
    //    insertUtil(df, "idx_es_charge_incoming_convert")
    //    insertUtil(df, "idx_es_charge_incoming_kou")
    //    df.unpersist(false)
  }

  def Job22(spark: SparkSession): Unit = {
    truncateTanleUtil("idx_es_charge_voucher_mast_refund_back")
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_INCOMING_BACK: String =
      """
        |SELECT
        |fld_area_guid,
        |fld_guid,
        |fld_incoming_back_guid,
        |substr( fld_submit_time, 1,19 ) fld_submit_time
        | FROM test.back1
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
    val s8 =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.fld_guid as ib_back_fld_guid,
        |  t2.fld_submit_time as ib_back_submit_time
        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_back_guid
        |""".stripMargin
    spark.sql(s8).createTempView("t8_tmp")

    val s8_ok =
      """
        |select m.fld_guid,m.ib_back_fld_guid
        |   from (
        |select *,row_number() over ( partition by fld_guid order by ib_back_submit_time desc)back1
        |  from t8_tmp
        |)m where m.back1=1
        |""".stripMargin
    spark.sql(s8_ok).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,ib_back_fld_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_voucher_mast_refund_back")
//    insertUtil(spark.sql(concatUtil("ib_back_fld_guid")), "idx_es_charge_voucher_mast_refund_back")
  }

  def Job23(spark: SparkSession): Unit = {
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_INCOMING_BACK: String =
      """
        |select
        |fld_area_guid,
        |fld_guid,
        |fld_incoming_convert_guid,
        |substr( fld_submit_time, 1,19 ) fld_submit_time
        | FROM test.back1
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
    val s8 =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.fld_guid as ib_convert_fld_guid,
        |  t2.fld_submit_time as ib_back_submit_time
        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_convert_guid
        |""".stripMargin
    spark.sql(s8).createTempView("t8_tmp")
    val s8_ok =
      """
        |select m.fld_guid,m.ib_convert_fld_guid
        |   from(
        |	   select fld_guid, ib_convert_fld_guid, row_number() over (partition by fld_guid order by ib_back_submit_time desc) back1
        |  	from t8_tmp
        |)m where m.back1=1
        |""".stripMargin
    spark.sql(s8_ok).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,ib_convert_fld_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_voucher_mast_refund_convert")
//    insertUtil(spark.sql(concatUtil("ib_convert_fld_guid")), "idx_es_charge_voucher_mast_refund_convert")
  }

  def Job24(spark: SparkSession): Unit = {
    //  es_charge_incoming_back b1 on b1.fld_incoming_back_guid=d.fld_incoming_fee_guid
    val mainStr =
      """
        |select
        | fld_guid,
        | fld_area_guid,fld_incoming_fee_guid
        | from test.all_tab
        |""".stripMargin
    val ES_CHARGE_INCOMING_BACK: String =
      """
        |select
        |fld_area_guid,
        |fld_guid,
        |fld_incoming_kou_guid,
        |substr( fld_submit_time, 1,19 ) fld_submit_time
        | FROM test.back1
        |""".stripMargin
    spark.sql(mainStr).createTempView("t1")
    spark.sql(ES_CHARGE_INCOMING_BACK).createTempView("t2")
    val s8 =
      """
        |select
        |  t1.fld_guid as fld_guid,
        |  t2.fld_guid as ib_kou_fld_guid,
        |  t2.fld_submit_time as ib_back_submit_time
        |  from t1 join t2 on t1.fld_incoming_fee_guid=t2.fld_incoming_kou_guid
        |""".stripMargin
    spark.sql(s8).createTempView("t8_tmp")
    val s8_ok =
      """
        |select m.fld_guid,m.ib_kou_fld_guid
        |   from(
        |select *,row_number() over ( partition by fld_guid order by ib_back_submit_time desc)back1
        |  from t8_tmp
        |)m where m.back1=1
        |""".stripMargin
    spark.sql(s8_ok).createTempView("xx")
    val insertSql =
      """
        |select
        | fld_guid,ib_kou_fld_guid as fld_value
        | from xx
        |""".stripMargin
    insertUtilPartition(spark, insertSql, "idx_es_charge_voucher_mast_refund_kou")
//    insertUtil(spark.sql(concatUtil("ib_kou_fld_guid")), "idx_es_charge_voucher_mast_refund_kou")
  }
}


