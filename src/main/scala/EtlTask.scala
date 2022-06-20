import com.sunac.Sql
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 */
object EtlTask {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hive")
//    val day: String = args(0)
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.warehouse.dir", "hdfs://HDPCluster:8020/user/hive/warehouse")
//      .config("hive.exec.dynamici.partition", true)
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .config("hive.metadata.dml.events", "false")
//      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
//      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled","true")
      .getOrCreate()
//    val bseg: DataFrame = spark.sql(Sql.bseg)
//    bseg.createTempView("bseg")
//
//    val bkpf: DataFrame = spark.sql(Sql.bkpf)
//    bkpf.createTempView("bkpf")
//
//    val ska1: DataFrame = spark.sql(Sql.ska1)
//    ska1.createTempView("ska1")
//
//    val csks: DataFrame = spark.sql(Sql.csks)
//    csks.createTempView("csks")
//
//    val cepc: DataFrame = spark.sql(Sql.cepc)
//    cepc.createTempView("cepc")
//
//    val ztfi0258: DataFrame = spark.sql(Sql.ztfi0258)
//    ztfi0258.createTempView("ztfi0258")
//
//    val kna1: DataFrame = spark.sql(Sql.kna1)
//    kna1.createTempView("kna1")
//
//    val lfa1: DataFrame = spark.sql(Sql.lfa1)
//    lfa1.createTempView("lfa1")
//
//    val ztfi0361: DataFrame = spark.sql(Sql.ztfi0361)
//    ztfi0361.createTempView("ztfi0361")
//
//    val ztfi0314: DataFrame = spark.sql(Sql.ztfi0314)
//    ztfi0314.createTempView("ztfi0314")
//
//    val ztfi_fkconf_log: DataFrame = spark.sql(Sql.ztfi_fkconf_log)
//    ztfi_fkconf_log.createTempView("ztfi_fkconf_log")
//
//    val ztfi3015: DataFrame = spark.sql(Sql.ztfi3015)
//    ztfi3015.createTempView("ztfi3015")
//
//    val ztfi0097: DataFrame = spark.sql(Sql.ztfi0097)
//    ztfi0097.createTempView("ztfi0097")
//
//    val ztfi3016: DataFrame = spark.sql(Sql.ztfi3016)
//    ztfi3016.createTempView("ztfi3016")
//
////    bseg.join(bkpf,["BELNR","BUKRS","GJAHR"],"left")
////    bseg.join(bkpf,new Seq[],"")
//
//    val mid1: DataFrame = spark.sql(
//      """
//        |select
//        |nvl(if(t2.XREF2_HD='-999','-',t2.XREF2_HD),'-')  as `bkpfXref2Hd` --融创凭证号
//        |,nvl(if(t2.ZYZID ='-999','-',t2.ZYZID ),'-')  as `bkpfZyzid` --预制凭证号
//        |,nvl(if(t2.BKTXT='-999','-',t2.BKTXT),'-')   as `bkpfBktxt` --凭证抬头文本
//        |,nvl(if(t1.BELNR='-999','-',t1.BELNR),'-')   as `bsegBelnr` --会计凭证号码
//        |,nvl(if(t2.NUMPG='-999','-',t2.NUMPG),'-')   as `bkpfNumpg`  --附件张数
//        |,nvl(if(t2.KURSF='-999','-',t2.KURSF),'-')   as `bkpfKursf` --汇率
//        |,nvl(if(t1.BUZEI='-999','-',t1.BUZEI),'-')   as `bsegbuzei` --行项目数
//        |,nvl(if(t1.BUKRS='-999','-',t1.BUKRS),'-')   as `bsegBukrs` --公司代码
//        |,nvl(if(t1.GJAHR='-999','-',t1.GJAHR),'-')   as `bsegGjahr` --财年
//        |,nvl(if(t1.H_MONAT='-999','-',t1.H_MONAT),'-')   as `bsegH2Monat` --会计期间
//        |,nvl(if(t1.H_BLDAT='-999','-',t1.H_BLDAT),'-')   as `bsegH2Bldat` --业务日期
//        |,nvl(if(t1.H_BUDAT='-999','-',t1.H_BUDAT),'-')   as `bsegH2Budat` --记帐日期
//        |,nvl(if(t1.H_BLART='-999','-',t1.H_BLART),'-')   as `bsegH2Bblart` --凭证类型
//        |,nvl(if(t1.SGTXT ='-999','-',t1.SGTXT ),'-')  as `bsegSgtxt` --摘要
//        |,nvl(if(t1.HKONT='-999','-',t1.HKONT),'-')   as `bsegHkont` --总分类帐帐目
//        |,nvl(if(t3.TXT50='-999','-',t3.TXT50),'-')   as `ska1Txt50` --总账科目长文本
//        |,nvl(if(t1.BSCHL='-999','-',t1.BSCHL),'-')   as `bsegBschl` --记帐代码
//        |,nvl(if(t1.H_HWAER='-999','-',t1.H_HWAER),'-')   as `bsegH2Hwaer` --本币
//        |,t1.bsegH2Hrbtr as `bsegH2Hrbtr` --借方金额
//        |,t1.bsegS2Wrbtr  as `bsegS2Wrbtr` --贷方金额
//        |,nvl(if(t1.GSBER='-999','-',t1.GSBER),'-')   as `bsegGsber` --业务类型
//        |,nvl(if(t1.SHKZG='-999','-',t1.SHKZG),'-')   as `bsegShkzg` --借方/贷方标识
//        |,nvl(if(t1.DMBTR='-999','-',t1.DMBTR),'-')   as `bsegDmbtr` --本位币金额
//        |,nvl(if(t1.KOSTL='-999','-',t1.KOSTL),'-')   as `bsegKostl` --成本中心
//        |,nvl(if(t1.PRCTR='-999','-',t1.PRCTR),'-')   as `bsegPrctr` --利润中心
//        |,nvl(if(t1.ZZRSTGR='-999','-',t1.ZZRSTGR),'-')   as `bsegZzrstgr` --现金流量码
//        |,nvl(if(t1.HKTID='-999','-',t1.HKTID),'-')   as `bsegHktid` --银行标识
//        |,nvl(if(t1.HBKID ='-999','-',t1.HBKID ),'-')  as `bsegHbkid` --开户银行
//        |,nvl(if(t1.KUNNR='-999','-',t1.KUNNR),'-')   as `bsegKunnr` --客户编号
//        |,nvl(if(t1.LIFNR='-999','-',t1.LIFNR),'-')   as `bsegLifnr` --供应商或债权人的帐号
//        |,nvl(if(t1.ANLN1 ='-999','-',t1.ANLN1 ),'-')  as `bsegAnln1` --主资产号
//        |,nvl(if(t1.ANBWA='-999','-',t1.ANBWA),'-')   as `bsegAnbwa` --资产业务类型
//        |,nvl(if(t1.ZZXIANGMU='-999','-',t1.ZZXIANGMU),'-')   as `bsegZzxiangmu` --项目
//        |,nvl(if(t1.ZZHETO='-999','-',t1.ZZHETO),'-')   as `bsegZzheto` --合同档案
//        |,nvl(if(t1.MWSKZ ='','-',t1.MWSKZ),'-')  as `bsegMwskz` --销售/购买税代码
//        |,nvl(if(t1.ZZBEIYONG1='-999','-',t1.ZZBEIYONG1),'-')   as `bsegZzbeiyong1` --辅助核算项
//        |,nvl(if(t1.ZZBEIYONG2='-999','-',t1.ZZBEIYONG2),'-')   as `bsegZzbeiyong2` --辅助核算类别
//        |,nvl(if(t1.ZFBDT='-999','-',t1.ZFBDT),'-')   as `bsegZfbdt` --基准日期
//        |,nvl(if(t1.ZZMENGE='-999','-',t1.ZZMENGE),'-')   as `bsegZzmenge` --往来类型
//        |,nvl(if(t1.MEINS ='-999','-',t1.MEINS ),'-')  as `bsegMeins` --基本计量单位
//        |,nvl(if(t1.XNEGP='-999','-',t1.XNEGP),'-')   as `bsegXnegp` --标识: 反记帐
//        |,nvl(if(t1.ZZHKTID='-999','-',t1.ZZHKTID),'-')   as `bsegZzhktid` --银行账号
//        |--原单据号
//        |,nvl(if(t2.TCODE='-999','-',t2.TCODE),'-')   as `bkpfTcode` --系统来源
//        |,nvl(if(t1.ZZLFINR='-999','-',t1.ZZLFINR),'-')   as `bsegZzlfinr` --供应商
//        |,nvl(if(t1.ZZSHULZ='-999','-',t1.ZZSHULZ),'-')   as `bsegZzshulz` --面积
//        |,nvl(if(t1.ZZKUNNR='-999','-',t1.ZZKUNNR),'-')   as `bsegZzkunnr` --客户
//        |,nvl(if(t1.ZZLTEXT ='-999','-',t1.ZZLTEXT ),'-')  as `bsegZzltext` --凭证长文本
//        |,nvl(if(t1.ZZWYFWLX='-999','-',t1.ZZWYFWLX),'-')   as `bsegZzwyfwlx` --物业服务类型
//        |,nvl(if(t1.ZZSKXZ ='-999','-',t1.ZZSKXZ ),'-')  as `bsegZzskxz` --税款性质
//        |,nvl(if(t1.ZZCOLLC='-999','-',t1.ZZCOLLC),'-')   as `bsegZzcollc` --代收费用类型
//        |,nvl(if(t2.XREVERSAL='-999','-',t2.XREVERSAL),'-')  as `bkpfXreversal` --凭证状态
//        |,nvl(if(t2.TJUSR='-999','-',t2.TJUSR),'-')   as `bkpfTjusr` --制单人
//        |,nvl(if(t2.TJDATE ='-999','-',t2.TJDATE ),'-')  as `bkpfTjdate` --制单日期
//        |,nvl(if(t2.TJTIME='-999','-',t2.TJTIME),'-')   as `bkpfTjtime` --制单时间
//        |,nvl(if(t2.GZUSR='','-',t2.GZUSR),'-')   as `bkpfGzusr` --过账人
//        |,nvl(if(t2.GZRDT='-999','-',t2.GZRDT),'-')   as `bkpfGzrdt` --过账日期
//        |,nvl(if(t2.GZTIM ='-999','-',t2.GZTIM ),'-')  as `bkpfGztim` --过账时间
//        |
//        |from
//        |bseg t1
//        |left join bkpf t2
//        |on t1.BELNR = t2.BELNR
//        |and t1.BUKRS = t2.BUKRS
//        |and t1.GJAHR = t2.GJAHR
//        |left join ska1 t3
//        |ON t1.HKONT = t3.SAKAN
//        |""".stripMargin)
//    mid1.createTempView("mid1")
//
//    val mid2: DataFrame = spark.sql(
//      """
//        |select
//        |t1.bkpfXref2Hd --融创凭证号
//        |,t1.bkpfZyzid --预制凭证号
//        |,t1.bkpfBktxt --凭证抬头文本
//        |,t1.bsegBelnr --会计凭证号码
//        |,t1.bkpfNumpg  --附件张数
//        |,t1.bkpfKursf --汇率
//        |,t1.bsegbuzei --行项目数
//        |,t1.bsegBukrs --公司代码
//        |,t1.bsegGjahr --财年
//        |,t1.bsegH2Monat --会计期间
//        |,t1.bsegH2Bldat --业务日期
//        |,t1.bsegH2Budat --记帐日期
//        |,t1.bsegH2Bblart --凭证类型
//        |,t1.bsegSgtxt --摘要
//        |,t1.bsegHkont --总分类帐帐目
//        |,t1.ska1Txt50 --总账科目长文本
//        |,t1.bsegBschl --记帐代码
//        |,t1.bsegH2Hwaer --本币
//        |,t1.bsegH2Hrbtr --借方金额
//        |,t1.bsegS2Wrbtr --贷方金额
//        |,t1.bsegGsber --业务类型
//        |,t1.bsegShkzg  --借方/贷方标识
//        |,t1.bsegDmbtr --本位币金额
//        |,t1.bsegKostl --成本中心
//        |,t1.bsegPrctr --利润中心
//        |,t1.bsegZzrstgr --现金流量码
//        |,t1.bsegHktid --银行标识
//        |,t1.bsegHbkid --开户银行
//        |,t1.bsegKunnr --客户编号
//        |,t1.bsegLifnr --供应商或债权人的帐号
//        |,t1.bsegAnln1 --主资产号
//        |,t1.bsegAnbwa --资产业务类型
//        |,t1.bsegZzxiangmu --项目
//        |,t1.bsegZzheto --合同档案
//        |,t1.bsegMwskz --销售/购买税代码
//        |,t1.bsegZzbeiyong1 --辅助核算项
//        |,t1.bsegZzbeiyong2 --辅助核算类别
//        |,t1.bsegZfbdt --基准日期
//        |,t1.bsegZzmenge --往来类型
//        |,t1.bsegMeins --基本计量单位
//        |,t1.bsegXnegp --标识: 反记帐
//        |,t1.bsegZzhktid --银行账号
//        |--原单据号
//        |,t1.bkpfTcode --系统来源
//        |,t1.bsegZzlfinr --供应商
//        |,t1.bsegZzshulz --面积
//        |,t1.bsegZzkunnr --客户
//        |,t1.bsegZzltext --凭证长文本
//        |,t1.bsegZzwyfwlx --物业服务类型
//        |,t1.bsegZzskxz --税款性质
//        |,t1.bsegZzcollc --代收费用类型
//        |,t1.bkpfXreversal --凭证状态
//        |,t1.bkpfTjusr --制单人
//        |,t1.bkpfTjdate --制单日期
//        |,t1.bkpfTjtime --制单时间
//        |,t1.bkpfGzusr --过账人
//        |,t1.bkpfGzrdt --过账日期
//        |,t1.bkpfGztim --过账时间
//        |,nvl(if(t4.ktext='-999','-',t4.ktext),'-') as `csksKtext`  --成本中心名称
//        |,nvl(if(t5.ktext='-999','-',t5.ktext),'-') as `cepcKtext`  --利润中心描述
//        |from mid1 t1
//        |LEFT JOIN csks t4
//        |ON t1.bsegKostl = t4.kostl
//        |LEFT JOIN cepc t5
//        |ON t1.bsegPrctr = t5.prctr
//
//        |""".stripMargin)
//    mid2.createTempView("mid2")
//    val mid3 =spark.sql("""
//       |select
//       |t1.bkpfXref2Hd --融创凭证号
//       |,t1.bkpfZyzid --预制凭证号
//       |,t1.bkpfBktxt --凭证抬头文本
//       |,t1.bsegBelnr --会计凭证号码
//       |,t1.bkpfNumpg  --附件张数
//       |,t1.bkpfKursf --汇率
//       |,t1.bsegbuzei --行项目数
//       |,t1.bsegBukrs --公司代码
//       |,t1.bsegGjahr --财年
//       |,t1.bsegH2Monat --会计期间
//       |,t1.bsegH2Bldat --业务日期
//       |,t1.bsegH2Budat --记帐日期
//       |,t1.bsegH2Bblart --凭证类型
//       |,t1.bsegSgtxt --摘要
//       |,t1.bsegHkont --总分类帐帐目
//       |,t1.ska1Txt50 --总账科目长文本
//       |,t1.bsegBschl --记帐代码
//       |,t1.bsegH2Hwaer --本币
//       |,t1.bsegH2Hrbtr --借方金额
//       |,t1.bsegS2Wrbtr --贷方金额
//       |,t1.bsegGsber --业务类型
//       |,t1.bsegShkzg  --借方/贷方标识
//       |,t1.bsegDmbtr --本位币金额
//       |,t1.bsegKostl --成本中心
//       |,t1.bsegPrctr --利润中心
//       |,t1.bsegZzrstgr --现金流量码
//       |,t1.bsegHktid --银行标识
//       |,t1.bsegHbkid --开户银行
//       |,t1.bsegKunnr --客户编号
//       |,t1.bsegLifnr --供应商或债权人的帐号
//       |,t1.bsegAnln1 --主资产号
//       |,t1.bsegAnbwa --资产业务类型
//       |,t1.bsegZzxiangmu --项目
//       |,t1.bsegZzheto --合同档案
//       |,t1.bsegMwskz --销售/购买税代码
//       |,t1.bsegZzbeiyong1 --辅助核算项
//       |,t1.bsegZzbeiyong2 --辅助核算类别
//       |,t1.bsegZfbdt --基准日期
//       |,t1.bsegZzmenge --往来类型
//       |,t1.bsegMeins --基本计量单位
//       |,t1.bsegXnegp --标识: 反记帐
//       |,t1.bsegZzhktid --银行账号
//       |--原单据号
//       |,t1.bkpfTcode --系统来源
//       |,t1.bsegZzlfinr --供应商
//       |,t1.bsegZzshulz --面积
//       |,t1.bsegZzkunnr --客户
//       |,t1.bsegZzltext --凭证长文本
//       |,t1.bsegZzwyfwlx --物业服务类型
//       |,t1.bsegZzskxz --税款性质
//       |,t1.bsegZzcollc --代收费用类型
//       |,t1.bkpfXreversal --凭证状态
//       |,t1.bkpfTjusr --制单人
//       |,t1.bkpfTjdate --制单日期
//       |,t1.bkpfTjtime --制单时间
//       |,t1.bkpfGzusr --过账人
//       |,t1.bkpfGzrdt --过账日期
//       |,t1.bkpfGztim --过账时间
//       |,t1.csksKtext  --成本中心名称
//       |,t1.cepcKtext --利润中心描述
//       |,nvl(if(t6.rstgt='-999','-',t6.rstgt),'-')   as `ztfi0258Rstgt`  --现金流量描述
//       |,nvl(if(t7.name1='-999','-',t7.name1),'-')   as `kna1Name1`  --客户描述
//       |,nvl(if(t8.name1='-999','-',t8.name1),'-')   as `lfa1Name1`  --供应商描述
//       |from mid2 t1
//       |LEFT JOIN ztfi0258 t6
//       |ON t1.bsegzzrstgr = t6.rstgr
//       |LEFT JOIN kna1 t7
//       |ON t1.bsegzzkunnr = t7.kunnr
//       |LEFT JOIN lfa1 t8
//       |ON t1.bsegzzlfinr = t8.lifnr
//       |""".stripMargin)
//    mid3.createTempView("mid3")
//
//    spark.sql(
//      """
//        |create table sunac.t_mid_tmp as
//        |select * from mid3
//        |""".stripMargin)
//      val mid4 = spark.sql(
//        """
//          |select
//          |t1.bkpfXref2Hd --融创凭证号
//          |,t1.bkpfZyzid --预制凭证号
//          |,t1.bkpfBktxt --凭证抬头文本
//          |,t1.bsegBelnr --会计凭证号码
//          |,t1.bkpfNumpg  --附件张数
//          |,t1.bkpfKursf --汇率
//          |,t1.bsegbuzei --行项目数
//          |,t1.bsegBukrs --公司代码
//          |,t1.bsegGjahr --财年
//          |,t1.bsegH2Monat --会计期间
//          |,t1.bsegH2Bldat --业务日期
//          |,t1.bsegH2Budat --记帐日期
//          |,t1.bsegH2Bblart --凭证类型
//          |,t1.bsegSgtxt --摘要
//          |,t1.bsegHkont --总分类帐帐目
//          |,t1.ska1Txt50 --总账科目长文本
//          |,t1.bsegBschl --记帐代码
//          |,t1.bsegH2Hwaer --本币
//          |,t1.bsegH2Hrbtr --借方金额
//          |,t1.bsegS2Wrbtr --贷方金额
//          |,t1.bsegGsber --业务类型
//          |,t1.bsegShkzg  --借方/贷方标识
//          |,t1.bsegDmbtr --本位币金额
//          |,t1.bsegKostl --成本中心
//          |,t1.bsegPrctr --利润中心
//          |,t1.bsegZzrstgr --现金流量码
//          |,t1.bsegHktid --银行标识
//          |,t1.bsegHbkid --开户银行
//          |,t1.bsegKunnr --客户编号
//          |,t1.bsegLifnr --供应商或债权人的帐号
//          |,t1.bsegAnln1 --主资产号
//          |,t1.bsegAnbwa --资产业务类型
//          |,t1.bsegZzxiangmu --项目
//          |,t1.bsegZzheto --合同档案
//          |,t1.bsegMwskz --销售/购买税代码
//          |,t1.bsegZzbeiyong1 --辅助核算项
//          |,t1.bsegZzbeiyong2 --辅助核算类别
//          |,t1.bsegZfbdt --基准日期
//          |,t1.bsegZzmenge --往来类型
//          |,t1.bsegMeins --基本计量单位
//          |,t1.bsegXnegp --标识: 反记帐
//          |,t1.bsegZzhktid --银行账号
//          |,nvl(if((case
//          |	when t1.bkpfTcode=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))
//          |	when t1.bkpfTcode=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))
//          |	when t1.bkpfTcode=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))
//          |else '-' end )='-999','-',(case
//          |	when t1.bkpfTcode=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))
//          |	when t1.bkpfTcode=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))
//          |	when t1.bkpfTcode=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))
//          |else '-' end)),'-') as `bsegBxdnr` --原单据号
//          |,t1.bkpfTcode --系统来源
//          |,t1.bsegZzlfinr --供应商
//          |,t1.bsegZzshulz --面积
//          |,t1.bsegZzkunnr --客户
//          |,t1.bsegZzltext --凭证长文本
//          |,t1.bsegZzwyfwlx --物业服务类型
//          |,t1.bsegZzskxz --税款性质
//          |,t1.bsegZzcollc --代收费用类型
//          |,t1.bkpfXreversal --凭证状态
//          |,t1.bkpfTjusr --制单人
//          |,t1.bkpfTjdate --制单日期
//          |,t1.bkpfTjtime --制单时间
//          |,t1.bkpfGzusr --过账人
//          |,t1.bkpfGzrdt --过账日期
//          |,t1.bkpfGztim --过账时间
//          |,t1.csksKtext  --成本中心名称
//          |,t1.cepcKtext --利润中心描述
//          |,t1.ztfi0258Rstgt  --现金流量描述
//          |,t1.kna1Name1  --客户描述
//          |,t1.lfa1Name1  --供应商描述
//          |from mid3 t1
//          |LEFT JOIN ztfi0361 t9
//          |ON t1.bsegbelnr = t9.belnr
//          |AND t1.bsegbukrs = t9.bukrs
//          |AND t1.bseggjahr = t9.gjahr
//          |LEFT JOIN ztfi0314 t10
//          |ON t1.bsegbelnr = t9.belnr
//          |AND t1.bsegbukrs = t9.bukrs
//          |AND t1.bseggjahr = t9.gjahr
//          |LEFT JOIN ztfi_fkconf_log t11
//          |ON t1.bsegbelnr = t11.belnr
//          |AND t1.bsegbukrs = t11.bukrs
//          |AND t1.bseggjahr = t11.gjahr
//          |LEFT JOIN ztfi3015 t12
//          |ON t1.bsegbelnr = t12.belnr
//          |AND t1.bsegbukrs = t12.bukrs
//          |AND t1.bseggjahr = t12.gjahr
//          |LEFT JOIN ztfi0097 t13
//          |ON t1.bsegbelnr = t13.belnr
//          |AND t1.bsegbukrs = t13.bukrs
//          |AND t1.bseggjahr = t13.gjahr
//          |LEFT JOIN ztfi3016 t14
//          |ON t1.bsegbelnr = t14.belnr
//          |AND t1.bsegbukrs = t14.bukrs
//          |AND t1.bseggjahr = t14.gjahr
//          |""".stripMargin)
//    mid1.show(10)
//    mid2.show(10)
//    mid4.show(10)
//    mid4.createTempView("mid4")
//    spark.sql(
//      """
//        |INSERT OVERWRITE TABLE sunac.ads_fd_sap_hs_journal_di partition (stat_date=20220615)
//        |select
//        |t1.bkpfXref2Hd --融创凭证号
//        |,t1.bkpfZyzid --预制凭证号
//        |,t1.bkpfBktxt --凭证抬头文本
//        |,t1.bsegBelnr --会计凭证号码
//        |,t1.bkpfNumpg  --附件张数
//        |,t1.bkpfKursf --汇率
//        |,t1.bsegbuzei --行项目数
//        |,t1.bsegBukrs --公司代码
//        |,t1.bsegGjahr --财年
//        |,t1.bsegH2Monat --会计期间
//        |,t1.bsegH2Bldat --业务日期
//        |,t1.bsegH2Budat --记帐日期
//        |,t1.bsegH2Bblart --凭证类型
//        |,t1.bsegSgtxt --摘要
//        |,t1.bsegHkont --总分类帐帐目
//        |,t1.ska1Txt50 --总账科目长文本
//        |,t1.bsegBschl --记帐代码
//        |,t1.bsegH2Hwaer --本币
//        |,t1.bsegH2Wrbtr --借方金额
//        |,t1.bsegS2Wrbtr --贷方金额
//        |,t1.bsegGsber --业务类型
//        |,t1.bsegShkzg  --借方/贷方标识
//        |,t1.bsegDmbtr --本位币金额
//        |,t1.bsegKostl --成本中心
//        |,t1.bsegPrctr --利润中心
//        |,t1.bsegZzrstgr --现金流量码
//        |,t1.bsegHktid --银行标识
//        |,t1.bsegHbkid --开户银行
//        |,t1.bsegKunnr --客户编号
//        |,t1.bsegLifnr --供应商或债权人的帐号
//        |,t1.bsegAnln1 --主资产号
//        |,t1.bsegAnbwa --资产业务类型
//        |,t1.bsegZzxiangmu --项目
//        |,t1.bsegZzheto --合同档案
//        |,t1.bsegMwskz --销售/购买税代码
//        |,t1.bsegZzbeiyong1 --辅助核算项
//        |,t1.bsegZzbeiyong2 --辅助核算类别
//        |,t1.bsegZfbdt --基准日期
//        |,t1.bsegZzmenge --往来类型
//        |,t1.bsegMeins --基本计量单位
//        |,t1.bsegXnegp --标识: 反记帐
//        |,t1.bsegZzhktid --银行账号
//        |,t1.bsegBxdnr --原单据号
//        |,t1.bkpfTcode --系统来源
//        |,t1.bsegZzlfinr --供应商
//        |,t1.bsegZzshulz --面积
//        |,t1.bsegZzkunnr --客户
//        |,t1.bsegZzltext --凭证长文本
//        |,t1.bsegZzwyfwlx --物业服务类型
//        |,t1.bsegZzskxz --税款性质
//        |,t1.bsegZzcollc --代收费用类型
//        |,t1.bkpfXreversal --凭证状态
//        |,t1.bkpfTjusr --制单人
//        |,t1.bkpfTjdate --制单日期
//        |,t1.bkpfTjtime --制单时间
//        |,t1.bkpfGzusr --过账人
//        |,t1.bkpfGzrdt --过账日期
//        |,t1.bkpfGztim --过账时间
//        |,t1.csksKtext  --成本中心名称
//        |,t1.cepcKtext --利润中心描述
//        |,t1.ztfi0258Rstgt  --现金流量描述
//        |,t1.kna1Name1  --客户描述
//        |,t1.lfa1Name1  --供应商描述
//        | from mid4 t1
//        |""".stripMargin)
//    sql2.createTempView("table2")
//    drop1.show(10)
    val oneDay: DataFrame = spark.sql(Sql.sqlOneDay)
    oneDay.show(1000)
//    println(sql.count())
    //    spark.sql(ec_common_use_func).createTempView("ec_common_use_func")
//    Config.insertUtil(spark, day, "A011_004_004", A011_004_004, true)

    spark.stop()
  }

}
