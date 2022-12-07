package com.sunac

import org.apache.spark.sql.functions.{col, rand}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SqlConfig_1 {
  val prop = new java.util.Properties
  //  prop.setProperty("user", "prod_readonly_for_sjzt")
  //  prop.setProperty("password", "Sunac@1918")
  //  prop.setProperty("user", "sjzt_dml")
  prop.setProperty("user", "root")
  //  prop.setProperty("password", "SJZT_dml")
  prop.setProperty("password", "UAT_tidb")
  prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  //  prop.setProperty("fetchSize", "1000")

  val JDBC_STR = "jdbc:mysql://10.3.72.83:4000/maindb?characterEncoding=utf-8"
  //  val JDBC_STR = "jdbc:mysql://172.17.44.84:4000/maindb?characterEncoding=utf-8"
  val JDBC_STR_2 = "jdbc:mysql://10.3.72.83:4000/sjzt?characterEncoding=utf-8&rewriteBatchedStatements=true"

  var ES_CHARGE_INCOMING_DATA_BILL: String =
    s"""
       |SELECT fld_guid,fld_data_src_guid,fld_bill_guid,fld_area_guid
       |FROM test.table_bill
       |where fld_data_src_guid IS NOT NULL and fld_bill_guid IS NOT NULL
       |""".stripMargin

  var ES_CHARGE_BILL: String =
    s"""
       |SELECT fld_guid,fld_status,fld_bill_code,fld_type_guid,substr(fld_operate_date,1,19) fld_operate_date,fld_area_guid
       |FROM  test.table_b
       |where fld_type_guid is not null
       |""".stripMargin

  var ES_CHARGE_BILL_TYPE: String =
    """
      |SELECT fld_guid,fld_category,fld_name FROM test.table_t where fld_category in (0,1)
      |""".stripMargin

  val SLIDE_SQL =
    """
      |SELECT  %s  FROM  maindb.%s group by %s
      |""".stripMargin

  val SLIDE_SQL2 =
    """
      |SELECT substr( fld_start_date, '%%Y-%%m' )  FROM  maindb.%s group by substr( fld_start_date, '%%Y-%%m' )
      |""".stripMargin

  def getBestArray(spark: SparkSession, name: String): Array[String] = {
    val countSQL =
      """
        |SELECT count(1) AS num FROM maindb.%s
        |""".stripMargin.format(name)
    val dataCount: Int = spark
      .read
      .format("jdbc")
      .jdbc(JDBC_STR, countSQL, prop).first().getLong(0).asInstanceOf[Int]
    // 获取切片策略：默认是500个分片
    getPartition(dataCount)
      .map {
        case (start, end) => s" fld_guid IN SELECT fld_guid FROM ${name} LIMIT ${start},${end})"
        //        case (start, end) => s" 1=1 LIMIT ${start},${end})"
      }.filter(arr => arr.length != 2)
  }

  def getPartition(count: Int): Array[(Int, Int)] = {
    val step = count / 1000
    if (step <= 1000) {
      return Array((0, count))
    }
    Range(0, count, step).map(x => {
      (x, step)
    }).toArray
  }

  // 1,
  var ES_CHARGE_INCOMING_DATA_2: String =
    """
      |SELECT
      |       fld_guid,
      |       fld_create_user,
      |       substr( fld_create_date, 1,19 ) fld_create_date,
      |       fld_modify_user,
      |       substr( fld_modify_date, 1,19 ) fld_modify_date,
      |       fld_tenancy,
      |       fld_incoming_fee_guid,
      |       fld_owner_fee_guid,
      |       fld_area_guid,
      |       fld_object_guid,
      |       fld_owner_guid,
      |       fld_project_guid,
      |       fld_pay_mode_guid,
      |       substr( fld_owner_date, 1,19) fld_owner_date,
      |       substr( fld_allot_date, 1,19 ) fld_allot_date,
      |       substr( fld_finance_date, 1,19 ) fld_finance_date,
      |       substr( fld_point_date, 1,19 ) fld_point_date,
      |       fld_desc,
      |       fld_total,
      |       fld_amount,
      |       fld_late_fee,
      |       fld_tax_amount,
      |       fld_tax,
      |       fld_bill_no,
      |       fld_bill_no2,
      |       fld_operator_guid,
      |       substr( fld_operate_date, 1,19 ) fld_operate_date,
      |       fld_back_fee_guid,
      |       fld_cancel,
      |       substr( fld_start_date, 1,19 ) fld_start_date,
      |       substr( fld_end_date, 1,19 ) fld_end_date,
      |       fld_back_guid
      | from test.all_tab
      |""".stripMargin

  var ES_CHARGE_INCOMING_DATA: String =
    """
      |select
      |       fld_guid,
      |       fld_create_user,
      |       substr(fld_create_date,1,19) fld_create_date,
      |       fld_modify_user,
      |       substr(fld_modify_date,1,19) fld_modify_date,
      |       fld_tenancy,
      |       fld_incoming_fee_guid,
      |       fld_owner_fee_guid,
      |       fld_area_guid,
      |       fld_object_guid,
      |       fld_owner_guid,
      |       fld_project_guid,
      |       fld_pay_mode_guid,
      |       fld_owner_date,
      |       fld_allot_date,
      |       fld_finance_date,
      |       fld_point_date,
      |       fld_desc,
      |       fld_total,
      |       fld_amount,
      |       fld_late_fee,
      |       fld_tax_amount,
      |       fld_tax,
      |       fld_bill_no,
      |       fld_bill_no2,
      |       fld_operator_guid,
      |       substr(fld_operate_date,1,19) fld_operate_date,
      |       fld_back_fee_guid,
      |       fld_cancel,
      |       fld_start_date,
      |       fld_end_date,
      |       fld_back_guid from test.all_tab
      |""".stripMargin



  // 2,


  var ES_CHARGE_INCOMING_FEE1: String =
    """
      |SELECT fld_guid,fld_busi_type,
      |fld_remark,
      |substr(fld_cancel_date,1,19) fld_cancel_date,
      |fld_cancel_guid,
      |fld_number,
      |fld_checkin,
      |fld_cancel_me,
      |fld_area_guid  FROM test.fee
      | where fld_guid is not null
      |""".stripMargin

  var ES_INFO_AREA_INFO: String =
    """
      |SELECT fld_guid,
      |fld_dq,
      |fld_fee_type,
      |substr(fld_confirm_date,1,19) fld_confirm_date,
      |fld_company,
      |fld_xm,
      |fld_ywdy,
      |fld_yt,
      |fld_name,
      |fld_area_guid FROM test.area
      | where fld_guid IS NOT NULL
      |""".stripMargin


  var ES_INFO_OBJECT: String =
    """
      |SELECT fld_guid,
      |fld_name AS fld_object_name,
      |substr( fld_owner_fee_date, 1,19 ) fld_owner_fee_date,
      |fld_building,
      |fld_cell,
      |fld_batch,
      |fld_charged_area,
      |fld_status AS fld_obj_status,
      |fld_class_guid,
      |fld_order,
      |fld_area_guid
      | FROM test.object where fld_guid IS NOT NULL
      |""".stripMargin

  var ES_INFO_OWNER: String =
    """
      |SELECT
      | fld_guid,
      | fld_name AS fld_owner_name,
      | fld_desc AS fld_owner_desc,
      | fld_area_guid
      | FROM test.owner
      | where fld_guid IS NOT NULL
      |""".stripMargin

  var ES_CHARGE_INCOMING_FEE: String =
    """
      |SELECT  fld_guid,  fld_busi_type,  fld_area_guid,  fld_remark,
      |substr(fld_cancel_date,1,19) as fld_cancel_date ,
      |fld_cancel_guid,   fld_cancel_me,    fld_checkin,   fld_number
      |from es_charge_incoming_fee 
      |""".stripMargin

  var ES_CHARGE_PROJECT: String =
    """
      |SELECT
      |fld_guid,
      |fld_name,
      |fld_object_type
      | FROM test.project where fld_guid IS NOT NULL
      |""".stripMargin

  var ES_CHARGE_PAY_MODE: String =
    """
      |SELECT fld_guid,
      |fld_name,
      |fld_resource,
      |fld_pre_pay,
      |fld_attribute FROM test.mode
      | where fld_guid IS NOT NULL
      |""".stripMargin

  var ES_CHARGE_OWNER_FEE: String =
    """
      |SELECT
      |fld_guid,
      |fld_area_guid,
      |fld_reason_remark,
      |fld_price,
      |fld_rebate,
      |fld_resource,
      |fld_desc,
      |fld_adjust_guid FROM test.cof
      |""".stripMargin


  var ES_INFO_OBJECT_PARK: String =
    """
      |SELECT fld_guid,fld_area_guid,fld_cw_category FROM test.park
      |""".stripMargin

  var ES_INFO_OBJECT_CLASS: String =
    """
      |SELECT fld_guid,fld_name FROM test.class where fld_guid IS NOT NULL
      |""".stripMargin

  var ES_INFO_OBJECT_AND_OWNER: String =
    """
      |SELECT fld_guid,fld_room_type,fld_is_owner,fld_area_guid,fld_object_guid,fld_owner_guid,
      |fld_is_current,fld_is_charge,fld_status FROM test.obj_owner 
      |""".stripMargin

  var ES_CHARGE_SETTLE_ACCOUNTS_DETAIL: String =
    """
      |SELECT fld_guid,fld_owner_fee_guid,fld_adjust_guid,fld_area_guid,fld_main_guid,fld_status,fld_settle_status,
      |fld_bill_no,substr(fld_create_date,1,19) fld_create_date FROM test.detail
      | WHERE fld_status=1 and fld_area_guid is not null
      |""".stripMargin

  var ES_CHARGE_SETTLE_ACCOUNT_SMAIN: String =
    """
      |SELECT fld_guid,fld_area_guid,fld_attribute,fld_examine_status
      | FROM test.main
      | where fld_examine_status=4 and fld_area_guid is not null
      |""".stripMargin


  var ES_CHARGE_PROJECT_PERIOD_JOIN: String =
    """
      |SELECT fld_guid,fld_project_guid,fld_period_guid FROM test.project_join
      |""".stripMargin


  var ES_CHARGE_PROJECT_PERIOD_PROJECT: String =
    """
      |SELECT fld_guid,fld_type,fld_name FROM test.project2 where fld_type=1
      |""".stripMargin

  var ES_CHARGE_TWO_BALANCE: String =
    """
      |SELECT fld_guid,substr( fld_date, 1,19 ) fld_date,fld_create_user,fld_hand_in_guid,fld_area_guid FROM test.two
      |""".stripMargin

  var ES_CHARGE_HAND_IN_RECORD: String =
    """
      |SELECT fld_guid,fld_hand_in,substr( fld_start_date, 1,19 ) fld_start_date,
      |substr( fld_end_date, 1,19 ) fld_end_date,fld_area_guid
      |FROM test.hir
      |""".stripMargin
  var ES_CHARGE_INCOMING_FEE2: String =
    """
      |SELECT fld_guid,fld_area_guid,
      |substr(fld_operate_date,1,19) fld_operate_date,
      |fld_cancel,
      |fld_create_user  FROM test.fee where fld_cancel!= 1 and fld_create_user != "importuser" and fld_create_user != "outuser"
      |""".stripMargin
  var ES_CHARGE_VOUCHER_CHECK_SERVICE_SET: String =
    """
      |SELECT fld_guid,fld_project_class_guid,fld_type FROM test.check where fld_project_class_guid is not null
      |""".stripMargin
  var ES_CHARGE_VOUCHER_PROJECT_PAY: String =
    """
      |SELECT fld_guid,fld_pay_mode_guid  FROM test.pay where fld_pay_mode_guid is not null
      |""".stripMargin

  var ES_CHARGE_INCOMING_BACK: String =
    """
      |SELECT
      |fld_area_guid,
      |fld_guid,
      |fld_incoming_back_guid,
      |fld_incoming_convert_guid,
      |fld_incoming_kou_guid,
      |substr( fld_submit_time, 1,19 ) fld_submit_time
      | FROM test.back1
      |""".stripMargin

  var ES_CHARGE_VOUCHER_MAST_REFUND: String =
    """
      |SELECT fld_guid,
      |fld_incoming_back_guid,
      |substr( fld_appay_date, 1,19 ) fld_appay_date,
      |substr( fld_date, 1,19 ) fld_date
      | FROM test.refund where fld_incoming_back_guid is not null
      |""".stripMargin

  def insertHiveUtil(df: DataFrame, name: String) = {
    df.repartitionByRange(200, col("fld_guid"), rand).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test." + name)
  }

  val INSERT_ALL_SQL =
    """
      |select
      |fld_guid
      |,fld_create_user
      |,fld_create_date
      |,fld_modify_user
      |,fld_modify_date
      |,fld_tenancy
      |,fld_incoming_fee_guid
      |,fld_owner_fee_guid
      |,fld_area_guid
      |,fld_object_guid
      |,fld_owner_guid
      |,fld_project_guid
      |,fld_pay_mode_guid
      |,fld_owner_date
      |,fld_allot_date
      |,fld_finance_date
      |,fld_point_date
      |,fld_desc
      |,fld_total
      |,fld_amount
      |,fld_late_fee
      |,fld_tax_amount
      |,fld_tax
      |,fld_bill_no
      |,fld_bill_no2
      |,fld_operator_guid
      |,fld_operate_date
      |,fld_back_fee_guid
      |,fld_cancel
      |,fld_start_date
      |,fld_end_date
      |,fld_back_guid
      |,f_fld_guid
      |,fld_busi_type
      |,fld_remark
      |,fld_cancel_date
      |,fld_cancel_guid
      |,fld_number
      |,fld_checkin
      |,fld_cancel_me
      |,a_fld_guid
      |,fld_area_name
      |,fld_dq
      |,fld_fee_type
      |,fld_confirm_date
      |,fld_company
      |,fld_xm
      |,fld_ywdy
      |,fld_yt
      |,o_fld_guid
      |,fld_object_name
      |,fld_owner_fee_date
      |,fld_building
      |,fld_cell
      |,fld_batch
      |,fld_charged_area
      |,fld_obj_status
      |,fld_object_class_guid
      |,obj_fld_order
      |,w_fld_guid
      |,fld_owner_name
      |,fld_owner_desc
      |,p_fld_guid
      |,fld_project_name
      |,fld_object_type
      |,m_fld_guid
      |,fld_pay_mode_name
      |,fld_resource
      |,fld_pre_pay
      |,pay_fld_attribute
      |,cof_fld_guid
      |,fld_reason_remark
      |,fld_price
      |,fld_rebate
      |,fld_owner_fee_resource
      |,fld_owner_fee_desc
      |,fld_fee_adjust_guid
      |,op_fld_guid
      |,fld_cw_category
      |,oc_fld_guid
      |,fld_object_class_name
      |,ao_fld_guid
      |,fld_is_owner
      |,fld_room_type
      |,db_fld_guid
      |,b_fld_guid
      |,t_fld_guid
      |,fld_bill_type
      |,fld_bill_status
      |,fld_bill_type_name
      |,fld_bill_code
      |,fld_bill_type2
      |,fld_bill_status2
      |,fld_bill_type_name2
      |,ad_fld_guid
      |,fld_status
      |,fld_adjust_guid
      |,fld_settle_status
      |,fld_accounts_detail_bill_no
      |,fld_main_guid
      |,am_fld_guid
      |,fld_attribute
      |,fld_examine_status
      |,pj_fld_guid
      |,fld_period_guid_join
      |,pp_fld_guid
      |,fld_project_period_name
      |,fld_balance_guid
      |,fld_hand_in_guid
      |,two_fld_date
      |,two_fld_create_user
      |,hir_fld_guid
      |,fld_hand_in
      |,fld_hand_in_start_date
      |,fld_hand_in_end_date
      |,f1_fld_guid
      |,firsttime
      |,ss_fld_guid
      |,fld_service_set_type
      |,vpp_fld_guid
      |,voucher_fld_pay_mode_guid
      |,ib_back_fld_guid
      |,back_fld_incoming_back_guid
      |,ib_convert_fld_guid
      |,back_fld_incoming_convert_guid as fld_incoming_back_guid
      |,fld_date
      |,ib_kou_fld_guid
      |,back_fld_incoming_kou_guid
      |,rf_back_fld_guid
      |,refund_fld_date_back
      |,rf_convert_fld_guid
      |,refund_fld_date_convert
      |,rf_kou_fld_guid
      |,refund_fld_date_kou from test.baozi6
      |""".stripMargin

  def initSpark(name: String): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "hive")
    SparkSession.builder()
      .appName(name)
      .config("spark.sql.warehouse.dir", "hdfs://HDPCluster:8020/user/hive/warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.blacklist.enabled", "true")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.yarn.max.executor.failures", 3)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
      //      .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", 60)
      //      .config("spark.dynamicAllocation.executorIdleTimeout", 60) //executor闲置时间
      //      .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", 60) //cache闲置时间
      // spark.debug.maxToStringFields
      .config("spark.task.maxFailures", 10)
      //      .master("local[1]")
      .enableHiveSupport()
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    println(ES_CHARGE_VOUCHER_MAST_REFUND)
  }
}
