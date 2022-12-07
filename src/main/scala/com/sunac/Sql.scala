package com.sunac

object Sql {

  var sql = "SELECT bukrs,gjahr,monat,stat_hour from sunac.ads_fd_sap_pbc_fixed_assets_hf limit 10"
  var sql1 = "show databases"
  var dropT1 = "DROP TABLE IF EXISTS sunac.t_mid_dwd_fd_sap_hs_bkpf"
  var dropT2 = "DROP TABLE IF EXISTS sunac.t_mid_dwd_fd_sap_hs_journal"
  var t1 = " create TABLE sunac.t_mid_dwd_fd_sap_hs_bkpf as\n with t9 as (\n  SELECT belnr,bukrs,gjahr,BXDNR\n  FROM sunac.dwd_fd_sap_hs_ztfi0361_df\n  WHERE stat_date = 20220615\n  GROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt10 as (\n  SELECT belnr,bukrs,gjahr,BXDNR\n  FROM sunac.dwd_fd_sap_hs_ztfi0314_df\n  WHERE stat_date = 20220615\n  GROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt11 as (\n  SELECT belnr,bukrs,gjahr,BXDNR\n  FROM sunac.dwd_fd_sap_hs_ztfi_fkconf_log_df \n  WHERE stat_date = 20220615\n  GROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt12 as (\n  SELECT belnr,bukrs,gjahr,BXDNR\n  FROM sunac.dwd_fd_sap_hs_ztfi3015_df\n  WHERE stat_date = 20220615\n),\nt13 as (\n  SELECT belnr,bukrs,gjahr,FKSQD_DBKEY\n  FROM sunac.dwd_fd_sap_hs_ztfi0097_df\n  WHERE stat_date = 20220615\n  GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY\n),\nt14 as (\n  SELECT belnr,bukrs,gjahr,FKSQD_DBKEY\n  FROM sunac.dwd_fd_sap_hs_ztfi3016_df\n  WHERE stat_date = 20220615\n  GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY\n) \nselect t2.belnr\n,t2.bukrs\n,t2.gjahr\n,t2.XREF2_HD  as `bkpfXref2Hd` --融创凭证号\n,t2.ZYZID  as `bkpfZyzid` --预制凭证号\n,t2.BKTXT as `bkpfBktxt` --凭证抬头文本\n,t2.NUMPG  as `bkpfNumpg`  --附件张数\n,t2.KURSF   as `bkpfKursf` --汇率\n,case \n  when t2.TCODE=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))\n  when t2.TCODE=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))\n  when t2.TCODE=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))\nelse '-' end as `bsegBxdnr` --原单据号\n,t2.TCODE   as `bkpfTcode` --系统来源\n,t2.XREVERSAL as `bkpfXreversal` --凭证状态\n,t2.TJUSR  as `bkpfTjusr` --制单人\n,t2.TJDATE  as `bkpfTjdate` --制单日期\n,t2.TJTIME   as `bkpfTjtime` --制单时间\n,t2.GZUSR  as `bkpfGzusr` --过账人\n,t2.GZRDT   as `bkpfGzrdt` --过账日期\n,t2.GZTIM as `bkpfGztim` --过账时间\n,t9.BXDNR as `t9bxdnr`\n,t10.BXDNR as `t10bxdnr`\n,t11.BXDNR as `t11bxdnr`\n,t12.BXDNR as `t12bxdnr`\n,t13.FKSQD_DBKEY as `t13FKSQD_DBKEY`\n,t14.FKSQD_DBKEY as `t14FKSQD_DBKEY`\nfrom sunac.dwd_fd_sap_hs_bkpf_di t2\nLEFT JOIN t9\nON t2.belnr = t9.belnr\nAND t2.bukrs = t9.bukrs\nAND t2.gjahr = t9.gjahr\nLEFT JOIN t10\nON t2.belnr = t9.belnr\nAND t2.bukrs = t9.bukrs\nAND t2.gjahr = t9.gjahr\nLEFT JOIN t11\nON t2.belnr = t11.belnr\nAND t2.bukrs = t11.bukrs\nAND t2.gjahr = t11.gjahr\nLEFT JOIN t12\nON t2.belnr = t12.belnr\nAND t2.bukrs = t12.bukrs\nAND t2.gjahr = t12.gjahr\nLEFT JOIN t13\nON t2.belnr = t13.belnr\nAND t2.bukrs = t13.bukrs\nAND t2.gjahr = t13.gjahr\nLEFT JOIN t14\nON t2.belnr = t14.belnr\nAND t2.bukrs = t14.bukrs\nAND t2.gjahr = t14.gjahr\nwhere t2.stat_date = 20220615"
  var t2 = " create TABLE sunac.t_mid_dwd_fd_sap_hs_journal as\n with t3 as (\n SELECT TXT50,SAKAN --6w\n FROM sunac.dwd_fd_sap_hs_ska1_df\n WHERE stat_date = 20220615\n GROUP BY TXT50,SAKAN\n),\nt4 as (\n SELECT kostl,ktext --1w\n FROM sunac.dwd_fd_sap_hs_csks_df\n WHERE stat_date = 20220615\n GROUP BY kostl,ktext\n),\nt5 as (\n SELECT prctr,ktext  --1w\n FROM sunac.dwd_fd_sap_hs_cepc_df\n WHERE stat_date = 20220615\n GROUP BY prctr,ktext\n),\nt6 as (\n SELECT rstgr,rstgt,zbblx  --351条\n FROM sunac.dwd_fd_sap_hs_ztfi0258_df\n WHERE zbblx = '08'\n AND stat_date = 20220615\n GROUP BY rstgr,rstgt,zbblx\n),\nt7 as (\n SELECT kunnr,name1  --33w+\n FROM sunac.dwd_fd_sap_hs_kna1_df\n WHERE stat_date = 20220615\n),\nt8 as (\n SELECT lifnr,name1   --44w+\n FROM sunac.dwd_fd_sap_hs_lfa1_df\n WHERE stat_date = 20220615\n)\nselect \nt2.bkpfXref2Hd\n,nvl(if(t2.bkpfZyzid='-999','-',t2.bkpfZyzid),'-')\n,t2.bkpfBktxt\n,t1.BELNR   as `bsegBelnr` --会计凭证号码\n,t2.bkpfNumpg\n,t2.bkpfKursf\n,t1.BUZEI  as `bsegbuzei` --行项目数\n,t1.BUKRS   as `bsegBukrs` --公司代码\n,t1.GJAHR   as `bsegGjahr` --财年\n,t1.H_MONAT  as `bsegH2Monat` --会计期间\n,t1.H_BLDAT   as `bsegH2Bldat` --业务日期\n,t1.H_BUDAT  as `bsegH2Budat` --记帐日期\n,t1.H_BLART   as `bsegH2Bblart` --凭证类型\n,t1.SGTXT  as `bsegSgtxt` --摘要\n,t1.HKONT   as `bsegHkont` --总分类帐帐目\n,nvl(if(t3.TXT50='-999','-',t3.TXT50),'-')   as `ska1Txt50` --总账科目长文本\n,t1.BSCHL   as `bsegBschl` --记帐代码\n,t1.H_HWAER  as `bsegH2Hwaer` --本币\n,IF (t1.SHKZG == 'S', WRBTR, '0') as `bsegH2Hwaer` --借方金额\n,IF (t1.SHKZG == 'H', WRBTR, '0') as `bsegS2Wrbtr` --贷方金额\n,nvl(if(t1.GSBER='-999','-',t1.GSBER),'-')   as `bsegGsber` --业务类型\n,t1.SHKZG  as `bsegShkzg ` --借方/贷方标识 \n,t1.DMBTR  as `bsegDmbtr` --本位币金额\n,nvl(if(t1.KOSTL='','-',t1.KOSTL),'-')   as `bsegKostl` --成本中心\n,nvl(if(t1.PRCTR='','-',t1.PRCTR),'-')   as `bsegPrctr` --利润中心\n,nvl(if(t1.ZZRSTGR='-999','-',t1.ZZRSTGR),'-')   as `bsegZzrstgr` --现金流量码\n,nvl(if(t1.HKTID='-999','-',t1.HKTID),'-')   as `bsegHktid` --银行标识\n,nvl(if(t1.HBKID ='-999','-',t1.HBKID ),'-')  as `bsegHbkid` --开户银行\n,nvl(if(t1.KUNNR='-999','-',t1.KUNNR),'-')   as `bsegKunnr` --客户编号\n,nvl(if(t1.LIFNR='-999','-',t1.LIFNR),'-')   as `bsegLifnr` --供应商或债权人的帐号\n,nvl(if(t1.ANLN1 ='-999','-',t1.ANLN1 ),'-')  as `bsegAnln1` --主资产号\n,nvl(if(t1.ANBWA='-999','-',t1.ANBWA),'-')   as `bsegAnbwa` --资产业务类型\n,nvl(if(t1.ZZXIANGMU='-999','-',t1.ZZXIANGMU),'-')   as `bsegZzxiangmu` --项目\n,nvl(if(t1.ZZHETO='-999','-',t1.ZZHETO),'-')   as `bsegZzheto` --合同档案\n,nvl(if(t1.MWSKZ ='','-',t1.MWSKZ ),'-')  as `bsegMwskz` --销售/购买税代码\n,nvl(if(t1.ZZBEIYONG1='-999','-',t1.ZZBEIYONG1),'-')   as `bsegZzbeiyong1` --辅助核算项\n,nvl(if(t1.ZZBEIYONG2='-999','-',t1.ZZBEIYONG2),'-')   as `bsegZzbeiyong2` --辅助核算类别\n,nvl(if(t1.ZFBDT='','-',t1.ZFBDT),'-')   as `bsegZfbdt` --基准日期\n,nvl(if(t1.ZZMENGE='-999','-',t1.ZZMENGE),'-')   as `bsegZzmenge` --往来类型\n,nvl(if(t1.MEINS ='-999','-',t1.MEINS ),'-')  as `bsegMeins` --基本计量单位\n,nvl(if(t1.XNEGP='-999','-',t1.XNEGP),'-')   as `bsegXnegp` --标识: 反记帐\n,nvl(if(t1.ZZHKTID='-999','-',t1.ZZHKTID),'-')   as `bsegZzhktid` --银行账号\n,t2.bsegBxdnr as `bsegBxdnr` --原单据号\n,t2.bkpfTcode\n,nvl(if(t1.ZZLFINR='-999','-',t1.ZZLFINR),'-')   as `bsegZzlfinr` --供应商\n,nvl(if(t1.ZZSHULZ='-999','-',t1.ZZSHULZ),'-')   as `bsegZzshulz` --面积\n,nvl(if(t1.ZZKUNNR='-999','-',t1.ZZKUNNR),'-')   as `bsegZzkunnr` --客户\n,nvl(if(t1.ZZLTEXT ='-999','-',t1.ZZLTEXT ),'-')  as `bsegZzltext` --凭证长文本\n,nvl(if(t1.ZZWYFWLX='-999','-',t1.ZZWYFWLX),'-')   as `bsegZzwyfwlx` --物业服务类型\n,nvl(if(t1.ZZSKXZ ='-999','-',t1.ZZSKXZ ),'-')  as `bsegZzskxz` --税款性质\n,nvl(if(t1.ZZCOLLC='-999','-',t1.ZZCOLLC),'-')   as `bsegZzcollc` --代收费用类型\n,t2.bkpfXreversal\n,t2.bkpfTjusr\n,t2.bkpfTjdate\n,t2.bkpfTjtime\n,nvl(if(t2.bkpfGzusr='-999','-',t2.bkpfGzusr),'-') \n,nvl(if(t2.bkpfGzrdt='-999','-',t2.bkpfGzrdt),'-')\n,nvl(if(t2.bkpfGztim='-999','-',t2.bkpfGztim),'-')\n,nvl(if(t4.ktext='-999','-',t4.ktext),'-')   as `csksKtext`  --成本中心名称\n,nvl(if(t5.ktext='-999','-',t5.ktext),'-')   as `cepcKtext`  --利润中心描述\n,nvl(if(t6.rstgt='-999','-',t6.rstgt),'-')   as `ztfi0258Rstgt`  --现金流量描述\n,nvl(if(t7.name1='-999','-',t7.name1),'-')   as `kna1Name1`  --客户描述\n,nvl(if(t8.name1='-999','-',t8.name1),'-')   as `lfa1Name1`  --供应商描述\n  from sunac.dwd_fd_sap_hs_bseg_di t1\nLEFT JOIN sunac.t_mid_dwd_fd_sap_hs_bkpf t2\nON t1.belnr = t2.belnr\nAND t1.bukrs = t2.bukrs\nAND t1.gjahr = t2.gjahr\nLEFT JOIN t3\nON t1.HKONT = t3.SAKAN\nLEFT JOIN t4\nON t4.kostl = t1.kostl \nLEFT JOIN t5\nON t1.prctr = t5.prctr \nLEFT JOIN t6\nON t1.zzrstgr = t6.rstgr \nLEFT JOIN t7\nON t1.zzkunnr = t7.kunnr \nLEFT JOIN t8\nON t1.zzlfinr = t8.lifnr\nwhere t1.stat_date = 20220615;"
  var good = "with t2 as (\n\tSELECT belnr,bukrs,gjahr,XREF2_HD,ZYZID,BKTXT,NUMPG,KURSF,TCODE,IF (XREVERSAL == 'X', '已冲销', '已过账') XREVERSAL,TJUSR,TJDATE,TJTIME,GZUSR,GZRDT,GZTIM\n\tfrom sunac.dwd_fd_sap_hs_bkpf_di\n\twhere stat_date = 20220615\n),\nt3 as (\n\tSELECT TXT50,SAKAN\n\tFROM sunac.dwd_fd_sap_hs_ska1_df\n\tWHERE stat_date = 20220615\n\tGROUP BY TXT50,SAKAN\n),\nt4 as (\n\tSELECT kostl,ktext\n\tFROM sunac.dwd_fd_sap_hs_csks_df\n\tWHERE stat_date = 20220615\n\tGROUP BY kostl,ktext\n),\nt5 as (\n\tSELECT prctr,ktext\n\tFROM sunac.dwd_fd_sap_hs_cepc_df\n\tWHERE stat_date = 20220615\n\tGROUP BY prctr,ktext\n),\nt6 as (\n\tSELECT rstgr,rstgt,zbblx\n\tFROM sunac.dwd_fd_sap_hs_ztfi0258_df\n\tWHERE zbblx = '08'\n\tAND stat_date = 20220615\n\tGROUP BY rstgr,rstgt,zbblx\n),\nt7 as (\n\tSELECT kunnr,name1\n\tFROM sunac.dwd_fd_sap_hs_kna1_df\n\tWHERE stat_date = 20220615\n\tGROUP BY kunnr,name1\n),\nt8 as (\n\tSELECT lifnr,name1\n\tFROM sunac.dwd_fd_sap_hs_lfa1_df\n\tWHERE stat_date = 20220615\n\tGROUP BY lifnr,name1\n),\nt9 as (\n\tSELECT belnr,bukrs,gjahr,BXDNR\n\tFROM sunac.dwd_fd_sap_hs_ztfi0361_df\n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt10 as (\n\tSELECT belnr,bukrs,gjahr,BXDNR\n\tFROM sunac.dwd_fd_sap_hs_ztfi0314_df\n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt11 as (\n\tSELECT belnr,bukrs,gjahr,BXDNR\n\tFROM sunac.dwd_fd_sap_hs_ztfi_fkconf_log_df \n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt12 as (\n\tSELECT belnr,bukrs,gjahr,BXDNR\n\tFROM sunac.dwd_fd_sap_hs_ztfi3015_df\n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,BXDNR\n),\nt13 as (\n\tSELECT belnr,bukrs,gjahr,FKSQD_DBKEY\n\tFROM sunac.dwd_fd_sap_hs_ztfi0097_df\n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY\n),\nt14 as (\n\tSELECT belnr,bukrs,gjahr,FKSQD_DBKEY\n\tFROM sunac.dwd_fd_sap_hs_ztfi3016_df\n\tWHERE stat_date = 20220615\n\tGROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY\n)\nINSERT OVERWRITE TABLE sunac.ads_fd_sap_hs_journal_di partition (stat_date=20220615)\nSELECT \nnvl(if(t2.XREF2_HD='','-',t2.XREF2_HD),'-')  as `bkpfXref2Hd` --融创凭证号\n,nvl(if(t2.ZYZID ='','-',t2.ZYZID ),'-')  as `bkpfZyzid` --预制凭证号\n,nvl(if(t2.BKTXT='','-',t2.BKTXT),'-')   as `bkpfBktxt` --凭证抬头文本\n,nvl(if(t1.BELNR='','-',t1.BELNR),'-')   as `bsegBelnr` --会计凭证号码\n,nvl(if(t2.NUMPG='','-',t2.NUMPG),'-')   as `bkpfNumpg`  --附件张数\n,nvl(if(t2.KURSF='','-',t2.KURSF),'-')   as `bkpfKursf` --汇率\n,nvl(if(t1.BUZEI='','-',t1.BUZEI),'-')   as `buzei` --行项目数\n,nvl(if(t1.BUKRS='','-',t1.BUKRS),'-')   as `bsegBukrs` --公司代码\n,nvl(if(t1.GJAHR='','-',t1.GJAHR),'-')   as `bsegGjahr` --财年\n,nvl(if(t1.H_MONAT='','-',t1.H_MONAT),'-')   as `bsegH2Monat` --会计期间\n,nvl(if(t1.H_BLDAT='','-',t1.H_BLDAT),'-')   as `bsegH2Bldat` --业务日期\n,nvl(if(t1.H_BUDAT='','-',t1.H_BUDAT),'-')   as `bsegH2Budat` --记帐日期\n,nvl(if(t1.H_BLART='','-',t1.H_BLART),'-')   as `bsegH2Bblart` --凭证类型\n,nvl(if(t1.SGTXT ='','-',t1.SGTXT ),'-')  as `bsegSgtxt` --摘要\n,nvl(if(t1.HKONT='','-',t1.HKONT),'-')   as `bsegHkont` --总分类帐帐目\n,nvl(if(t3.TXT50='-999','-',t3.TXT50),'-')   as `ska1Txt50` --总账科目长文本\n,nvl(if(t1.BSCHL='','-',t1.BSCHL),'-')   as `bsegBschl` --记帐代码\n,nvl(if(t1.H_HWAER='','-',t1.H_HWAER),'-')   as `bsegH2Hwaer` --本币\n,nvl(if(IF (t1.SHKZG == 'S', WRBTR, '0') ='','-',IF (t1.SHKZG == 'S', WRBTR, '0') ),'-') as `bsegH2Hwaer` --借方金额\n,nvl(if(IF (t1.SHKZG == 'H', WRBTR, '0')='','-',IF (t1.SHKZG == 'H', WRBTR, '0')),'-')  as `bsegS2Wrbtr` --贷方金额\n,nvl(if(t1.GSBER='','-',t1.GSBER),'-')   as `bsegGsber` --业务类型\n,nvl(if(t1.SHKZG='','-',t1.SHKZG),'-')   as `bsegShkzg ` --借方/贷方标识 \n,nvl(if(t1.DMBTR='','-',t1.DMBTR),'-')   as `bsegDmbtr` --本位币金额\n,nvl(if(t1.KOSTL='','-',t1.KOSTL),'-')   as `bsegKostl` --成本中心\n,nvl(if(t1.PRCTR='','-',t1.PRCTR),'-')   as `bsegPrctr` --利润中心\n,nvl(if(t1.ZZRSTGR='','-',t1.ZZRSTGR),'-')   as `bsegZzrstgr` --现金流量码\n,nvl(if(t1.HKTID='','-',t1.HKTID),'-')   as `bsegHktid` --银行标识\n,nvl(if(t1.HBKID ='','-',t1.HBKID ),'-')  as `bsegHbkid` --开户银行\n,nvl(if(t1.KUNNR='','-',t1.KUNNR),'-')   as `bsegKunnr` --客户编号\n,nvl(if(t1.LIFNR='','-',t1.LIFNR),'-')   as `bsegLifnr` --供应商或债权人的帐号\n,nvl(if(t1.ANLN1 ='','-',t1.ANLN1 ),'-')  as `bsegAnln1` --主资产号\n,nvl(if(t1.ANBWA='','-',t1.ANBWA),'-')   as `bsegAnbwa` --资产业务类型\n,nvl(if(t1.ZZXIANGMU='','-',t1.ZZXIANGMU),'-')   as `bsegZzxiangmu` --项目\n,nvl(if(t1.ZZHETO='','-',t1.ZZHETO),'-')   as `bsegZzheto` --合同档案\n,nvl(if(t1.MWSKZ ='','-',t1.MWSKZ ),'-')  as `bsegMwskz` --销售/购买税代码\n,nvl(if(t1.ZZBEIYONG1='','-',t1.ZZBEIYONG1),'-')   as `bsegZzbeiyong1` --辅助核算项\n,nvl(if(t1.ZZBEIYONG2='','-',t1.ZZBEIYONG2),'-')   as `bsegZzbeiyong2` --辅助核算类别\n,nvl(if(t1.ZFBDT='','-',t1.ZFBDT),'-')   as `bsegZfbdt` --基准日期\n,nvl(if(t1.ZZMENGE='','-',t1.ZZMENGE),'-')   as `bsegZzmenge` --往来类型\n,nvl(if(t1.MEINS ='','-',t1.MEINS ),'-')  as `bsegMeins` --基本计量单位\n,nvl(if(t1.XNEGP='','-',t1.XNEGP),'-')   as `bsegXnegp` --标识: 反记帐\n,nvl(if(t1.ZZHKTID='','-',t1.ZZHKTID),'-')   as `bsegZzhktid` --银行账号\n,nvl(if((case \n\twhen t2.TCODE=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))\n\twhen t2.TCODE=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))\n\twhen t2.TCODE=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))\nelse '-' end )='-999','-',(case \n\twhen t2.TCODE=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))\n\twhen t2.TCODE=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))\n\twhen t2.TCODE=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))\nelse '-' end)),'-') as `bsegBxdnr` --原单据号\n,nvl(if(t2.TCODE='','-',t2.TCODE),'-')   as `bkpfTcode` --系统来源\n,nvl(if(t1.ZZLFINR='','-',t1.ZZLFINR),'-')   as `bsegZzlfinr` --供应商\n,nvl(if(t1.ZZSHULZ='','-',t1.ZZSHULZ),'-')   as `bsegZzshulz` --面积\n,nvl(if(t1.ZZKUNNR='','-',t1.ZZKUNNR),'-')   as `bsegZzkunnr` --客户\n,nvl(if(t1.ZZLTEXT ='','-',t1.ZZLTEXT ),'-')  as `bsegZzltext` --凭证长文本\n,nvl(if(t1.ZZWYFWLX='','-',t1.ZZWYFWLX),'-')   as `bsegZzwyfwlx` --物业服务类型\n,nvl(if(t1.ZZSKXZ ='','-',t1.ZZSKXZ ),'-')  as `bsegZzskxz` --税款性质\n,nvl(if(t1.ZZCOLLC='','-',t1.ZZCOLLC),'-')   as `bsegZzcollc` --代收费用类型\n,nvl(if(t2.XREVERSAL='','-',t2.XREVERSAL),'-')  as `bkpfXreversal` --凭证状态\n,nvl(if(t2.TJUSR='','-',t2.TJUSR),'-')   as `bkpfTjusr` --制单人\n,nvl(if(t2.TJDATE ='','-',t2.TJDATE ),'-')  as `bkpfTjdate` --制单日期\n,nvl(if(t2.TJTIME='','-',t2.TJTIME),'-')   as `bkpfTjtime` --制单时间\n,nvl(if(t2.GZUSR='','-',t2.GZUSR),'-')   as `bkpfGzusr` --过账人\n,nvl(if(t2.GZRDT='','-',t2.GZRDT),'-')   as `bkpfGzrdt` --过账日期\n,nvl(if(t2.GZTIM ='','-',t2.GZTIM ),'-')  as `bkpfGztim` --过账时间\n,nvl(if(t4.ktext='-999','-',t4.ktext),'-')   as `csksKtext`  --成本中心名称\n,nvl(if(t5.ktext='-999','-',t5.ktext),'-')   as `cepcKtext`  --利润中心描述\n,nvl(if(t6.rstgt='-999','-',t6.rstgt),'-')   as `ztfi0258Rstgt`  --现金流量描述\n,nvl(if(t7.name1='-999','-',t7.name1),'-')   as `kna1Name1`  --客户描述\n,nvl(if(t8.name1='-999','-',t8.name1),'-')   as `lfa1Name1`  --供应商描述\nFROM \nsunac.dwd_fd_sap_hs_bseg_di t1\nLEFT JOIN t2\nON t1.belnr = t2.belnr\nAND t1.bukrs = t2.bukrs\nAND t1.gjahr = t2.gjahr\nLEFT JOIN t3\nON t1.HKONT = t3.SAKAN\nLEFT JOIN t4\nON t4.kostl = t1.kostl \nLEFT JOIN t5\nON t1.prctr = t5.prctr \nLEFT JOIN t6\nON t1.zzrstgr = t6.rstgr \nLEFT JOIN t7\nON t1.zzkunnr = t7.kunnr \nLEFT JOIN t8\nON t1.zzlfinr = t8.lifnr\nLEFT JOIN t9\nON t1.belnr = t9.belnr\nAND t1.bukrs = t9.bukrs\nAND t1.gjahr = t9.gjahr\nLEFT JOIN t10\nON t1.belnr = t9.belnr\nAND t1.bukrs = t9.bukrs\nAND t1.gjahr = t9.gjahr\nLEFT JOIN t11\nON t1.belnr = t11.belnr\nAND t1.bukrs = t11.bukrs\nAND t1.gjahr = t11.gjahr\nLEFT JOIN t12\nON t1.belnr = t12.belnr\nAND t1.bukrs = t12.bukrs\nAND t1.gjahr = t12.gjahr\nLEFT JOIN t13\nON t1.belnr = t13.belnr\nAND t1.bukrs = t13.bukrs\nAND t1.gjahr = t13.gjahr\nLEFT JOIN t14\nON t1.belnr = t14.belnr\nAND t1.bukrs = t14.bukrs\nAND t1.gjahr = t14.gjahr\nwhere t1.stat_date = 20220615"

  var bseg: String = """select BELNR
               |,BUZEI
               |,BUKRS
               |,GJAHR
               |,H_MONAT
               |,H_BLDAT
               |,H_BUDAT
               |,H_BLART
               |,SGTXT
               |,HKONT
               |,BSCHL
               |,H_HWAER
               |,IF (SHKZG == 'S', WRBTR, '0') as bsegH2Hrbtr
               |,IF (SHKZG == 'H', WRBTR, '0') as bsegS2Wrbtr
               |,GSBER
               |,SHKZG
               |,DMBTR
               |,KOSTL
               |,PRCTR
               |,ZZRSTGR
               |,HKTID
               |,HBKID
               |,KUNNR
               |,LIFNR
               |,ANLN1
               |,ANBWA
               |,ZZXIANGMU
               |,ZZHETO
               |,MWSKZ
               |,ZZBEIYONG1
               |,ZZBEIYONG2
               |,ZFBDT
               |,ZZMENGE
               |,MEINS
               |,XNEGP
               |,ZZHKTID
               |,ZZLFINR
               |,ZZSHULZ
               |,ZZKUNNR
               |,ZZLTEXT
               |,ZZWYFWLX
               |,ZZSKXZ
               |,ZZCOLLC
               |from sunac.dwd_fd_sap_hs_bseg_di where stat_date=20220615""".stripMargin
  var bkpf: String = """select BELNR
                 |,XREF2_HD
                 |,ZYZID
                 |,BUKRS
                 |,GJAHR
                 |,BKTXT
                 |,NUMPG
                 |,KURSF
                 |,TCODE
                 |,XREVERSAL
                 |,TJUSR
                 |,TJDATE
                 |,TJTIME
                 |,GZUSR
                 |,GZRDT
                 |,GZTIM
                 |from sunac.dwd_fd_sap_hs_bkpf_di where stat_date=20220615""".stripMargin
  var ska1: String = """SELECT TXT50,SAKAN
          |	FROM sunac.dwd_fd_sap_hs_ska1_df
          |	WHERE stat_date = 20220615
          |	GROUP BY TXT50,SAKAN""".stripMargin
  var csks: String = """SELECT kostl,ktext
          |	FROM sunac.dwd_fd_sap_hs_csks_df
          |	WHERE stat_date = 20220615
          |	GROUP BY kostl,ktext""".stripMargin
  var cepc: String = """SELECT prctr,ktext
                       |	FROM sunac.dwd_fd_sap_hs_cepc_df
                       |	WHERE stat_date = 20220615
                       |	GROUP BY prctr,ktext""".stripMargin
  var ztfi0258: String = """SELECT rstgr,rstgt,zbblx
                   |	FROM sunac.dwd_fd_sap_hs_ztfi0258_df
                   |	WHERE zbblx = '08'
                   |	AND stat_date = 20220615
                   |	GROUP BY rstgr,rstgt,zbblx""".stripMargin
  var kna1: String = """SELECT kunnr,name1
               |	FROM sunac.dwd_fd_sap_hs_kna1_df
               |	WHERE stat_date = 20220615
               |	GROUP BY kunnr,name1""".stripMargin
  var lfa1: String = """SELECT lifnr,name1
               |	FROM sunac.dwd_fd_sap_hs_lfa1_df
               |	WHERE stat_date = 20220615
               |	GROUP BY lifnr,name1""".stripMargin
  var ztfi0361: String = """SELECT belnr,bukrs,gjahr,BXDNR
               |	FROM sunac.dwd_fd_sap_hs_ztfi0361_df
               |	WHERE stat_date = 20220615
               |	GROUP BY belnr,bukrs,gjahr,BXDNR""".stripMargin
  var ztfi0314: String = """SELECT belnr,bukrs,gjahr,BXDNR
                   |	FROM sunac.dwd_fd_sap_hs_ztfi0314_df
                   |	WHERE stat_date = 20220615
                   |	GROUP BY belnr,bukrs,gjahr,BXDNR""".stripMargin
  var ztfi_fkconf_log: String = """SELECT belnr,bukrs,gjahr,BXDNR
               |	FROM sunac.dwd_fd_sap_hs_ztfi_fkconf_log_df
               |	WHERE stat_date = 20220615
               |	GROUP BY belnr,bukrs,gjahr,BXDNR""".stripMargin
  var ztfi3015: String = """SELECT belnr,bukrs,gjahr,BXDNR
          |	FROM sunac.dwd_fd_sap_hs_ztfi3015_df
          |	WHERE stat_date = 20220615
          |	GROUP BY belnr,bukrs,gjahr,BXDNR""".stripMargin

  var ztfi0097: String = """SELECT belnr,bukrs,gjahr,FKSQD_DBKEY
          |	FROM sunac.dwd_fd_sap_hs_ztfi0097_df
          |	WHERE stat_date = 20220615
          |	GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY""".stripMargin
  var ztfi3016: String = """SELECT belnr,bukrs,gjahr,FKSQD_DBKEY
          |	FROM sunac.dwd_fd_sap_hs_ztfi3016_df
          |	WHERE stat_date = 20220615
          |	GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY""".stripMargin

  var sqlOneDay: String =
    """
      |INSERT OVERWRITE TABLE sunac.ads_fd_sap_hs_journal_di partition (stat_date=20220615)
      |SELECT
      |nvl(if(t2.XREF2_HD='','-',t2.XREF2_HD),'-')  as `bkpfXref2Hd` --融创凭证号
      |,nvl(if(t2.ZYZID ='','-',t2.ZYZID ),'-')  as `bkpfZyzid` --预制凭证号
      |,nvl(if(t2.BKTXT='','-',t2.BKTXT),'-')   as `bkpfBktxt` --凭证抬头文本
      |,nvl(if(t1.BELNR='','-',t1.BELNR),'-')   as `bsegBelnr` --会计凭证号码
      |,nvl(if(t2.NUMPG='','-',t2.NUMPG),'-')   as `bkpfNumpg`  --附件张数
      |,nvl(if(t2.KURSF='','-',t2.KURSF),'-')   as `bkpfKursf` --汇率
      |,nvl(if(t1.BUZEI='','-',t1.BUZEI),'-')   as `bsegbuzei` --行项目数
      |,nvl(if(t1.BUKRS='','-',t1.BUKRS),'-')   as `bsegBukrs` --公司代码
      |,nvl(if(t1.GJAHR='','-',t1.GJAHR),'-')   as `bsegGjahr` --财年
      |,nvl(if(t1.H_MONAT='','-',t1.H_MONAT),'-')   as `bsegH2Monat` --会计期间
      |,nvl(if(t1.H_BLDAT='','-',t1.H_BLDAT),'-')   as `bsegH2Bldat` --业务日期
      |,nvl(if(t1.H_BUDAT='','-',t1.H_BUDAT),'-')   as `bsegH2Budat` --记帐日期
      |,nvl(if(t1.H_BLART='','-',t1.H_BLART),'-')   as `bsegH2Bblart` --凭证类型
      |,nvl(if(t1.SGTXT ='','-',t1.SGTXT ),'-')  as `bsegSgtxt` --摘要
      |,nvl(if(t1.HKONT='','-',t1.HKONT),'-')   as `bsegHkont` --总分类帐帐目
      |,nvl(if(t3.TXT50='-999','-',t3.TXT50),'-')   as `ska1Txt50` --总账科目长文本
      |,nvl(if(t1.BSCHL='','-',t1.BSCHL),'-')   as `bsegBschl` --记帐代码
      |,nvl(if(t1.H_HWAER='','-',t1.H_HWAER),'-')   as `bsegH2Hwaer` --本币
      |,nvl(if(IF (t1.SHKZG == 'S', t1.WRBTR, '0') ='','-',IF (t1.SHKZG == 'S', t1.WRBTR, '0') ),'-') as `bsegH2Hwaer` --借方金额
      |,nvl(if(IF (t1.SHKZG == 'H', t1.WRBTR, '0')='','-',IF (t1.SHKZG == 'H', t1.WRBTR, '0')),'-')  as `bsegS2Wrbtr` --贷方金额
      |,nvl(if(t1.GSBER='','-',t1.GSBER),'-')   as `bsegGsber` --业务类型
      |,nvl(if(t1.SHKZG='','-',t1.SHKZG),'-')   as `bsegShkzg ` --借方/贷方标识
      |,nvl(if(t1.DMBTR='','-',t1.DMBTR),'-')   as `bsegDmbtr` --本位币金额
      |,nvl(if(t1.KOSTL='','-',t1.KOSTL),'-')   as `bsegKostl` --成本中心
      |,nvl(if(t1.PRCTR='','-',t1.PRCTR),'-')   as `bsegPrctr` --利润中心
      |,nvl(if(t1.ZZRSTGR='','-',t1.ZZRSTGR),'-')   as `bsegZzrstgr` --现金流量码
      |,nvl(if(t1.HKTID='','-',t1.HKTID),'-')   as `bsegHktid` --银行标识
      |,nvl(if(t1.HBKID ='','-',t1.HBKID ),'-')  as `bsegHbkid` --开户银行
      |,nvl(if(t1.KUNNR='','-',t1.KUNNR),'-')   as `bsegKunnr` --客户编号
      |,nvl(if(t1.LIFNR='','-',t1.LIFNR),'-')   as `bsegLifnr` --供应商或债权人的帐号
      |,nvl(if(t1.ANLN1 ='','-',t1.ANLN1 ),'-')  as `bsegAnln1` --主资产号
      |,nvl(if(t1.ANBWA='','-',t1.ANBWA),'-')   as `bsegAnbwa` --资产业务类型
      |,nvl(if(t1.ZZXIANGMU='','-',t1.ZZXIANGMU),'-')   as `bsegZzxiangmu` --项目
      |,nvl(if(t1.ZZHETO='','-',t1.ZZHETO),'-')   as `bsegZzheto` --合同档案
      |,nvl(if(t1.MWSKZ ='','-',t1.MWSKZ ),'-')  as `bsegMwskz` --销售/购买税代码
      |,nvl(if(t1.ZZBEIYONG1='','-',t1.ZZBEIYONG1),'-')   as `bsegZzbeiyong1` --辅助核算项
      |,nvl(if(t1.ZZBEIYONG2='','-',t1.ZZBEIYONG2),'-')   as `bsegZzbeiyong2` --辅助核算类别
      |,nvl(if(t1.ZFBDT='','-',t1.ZFBDT),'-')   as `bsegZfbdt` --基准日期
      |,nvl(if(t1.ZZMENGE='','-',t1.ZZMENGE),'-')   as `bsegZzmenge` --往来类型
      |,nvl(if(t1.MEINS ='','-',t1.MEINS ),'-')  as `bsegMeins` --基本计量单位
      |,nvl(if(t1.XNEGP='','-',t1.XNEGP),'-')   as `bsegXnegp` --标识: 反记帐
      |,nvl(if(t1.ZZHKTID='','-',t1.ZZHKTID),'-')   as `bsegZzhktid` --银行账号
      |,nvl(if((case
      |	when t2.TCODE=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))
      |	when t2.TCODE=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))
      |	when t2.TCODE=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))
      |else '-' end )='-999','-',(case
      |	when t2.TCODE=='ZFID033' then (if (t9.BXDNR=='-999',t10.BXDNR,t9.BXDNR))
      |	when t2.TCODE=='ZFID037' then (if (t11.BXDNR=='-999',t12.BXDNR,t11.BXDNR))
      |	when t2.TCODE=='ZFID045' then (if (t13.FKSQD_DBKEY=='-999',t14.FKSQD_DBKEY,t13.FKSQD_DBKEY))
      |else '-' end)),'-') as `bsegBxdnr` --原单据号
      |,nvl(if(t2.TCODE='','-',t2.TCODE),'-')   as `bkpfTcode` --系统来源
      |,nvl(if(t1.ZZLFINR='','-',t1.ZZLFINR),'-')   as `bsegZzlfinr` --供应商
      |,nvl(if(t1.ZZSHULZ='','-',t1.ZZSHULZ),'-')   as `bsegZzshulz` --面积
      |,nvl(if(t1.ZZKUNNR='','-',t1.ZZKUNNR),'-')   as `bsegZzkunnr` --客户
      |,nvl(if(t1.ZZLTEXT ='','-',t1.ZZLTEXT ),'-')  as `bsegZzltext` --凭证长文本
      |,nvl(if(t1.ZZWYFWLX='','-',t1.ZZWYFWLX),'-')   as `bsegZzwyfwlx` --物业服务类型
      |,nvl(if(t1.ZZSKXZ ='','-',t1.ZZSKXZ ),'-')  as `bsegZzskxz` --税款性质
      |,nvl(if(t1.ZZCOLLC='','-',t1.ZZCOLLC),'-')   as `bsegZzcollc` --代收费用类型
      |,nvl(if(t2.XREVERSAL='','-',t2.XREVERSAL),'-')  as `bkpfXreversal` --凭证状态
      |,nvl(if(t2.TJUSR='','-',t2.TJUSR),'-')   as `bkpfTjusr` --制单人
      |,nvl(if(t2.TJDATE ='','-',t2.TJDATE ),'-')  as `bkpfTjdate` --制单日期
      |,nvl(if(t2.TJTIME='','-',t2.TJTIME),'-')   as `bkpfTjtime` --制单时间
      |,nvl(if(t2.GZUSR='','-',t2.GZUSR),'-')   as `bkpfGzusr` --过账人
      |,nvl(if(t2.GZRDT='','-',t2.GZRDT),'-')   as `bkpfGzrdt` --过账日期
      |,nvl(if(t2.GZTIM ='','-',t2.GZTIM ),'-')  as `bkpfGztim` --过账时间
      |,nvl(if(t4.ktext='-999','-',t4.ktext),'-')   as `csksKtext`  --成本中心名称
      |,nvl(if(t5.ktext='-999','-',t5.ktext),'-')   as `cepcKtext`  --利润中心描述
      |,nvl(if(t6.rstgt='-999','-',t6.rstgt),'-')   as `ztfi0258Rstgt`  --现金流量描述
      |,nvl(if(t7.name1='-999','-',t7.name1),'-')   as `kna1Name1`  --客户描述
      |,nvl(if(t8.name1='-999','-',t8.name1),'-')   as `lfa1Name1`  --供应商描述
      |FROM sunac.dwd_fd_sap_hs_bseg_di t1
      |left join sunac.dwd_fd_sap_hs_bkpf_di t2
      |ON t1.belnr = t2.belnr
      |AND t1.bukrs = t2.bukrs
      |AND t1.gjahr = t2.gjahr
      |and t2.stat_date= 20220615
      |LEFT join (
      |	SELECT SAKAN,TXT50
      |	FROM sunac.dwd_fd_sap_hs_ska1_df
      |	WHERE stat_date = 20220615
      |	GROUP BY SAKAN,TXT50
      |) t3
      |ON t1.HKONT = t3.SAKAN
      |left join (
      |	SELECT kostl,ktext
      |	FROM sunac.dwd_fd_sap_hs_csks_df
      |	WHERE stat_date = 20220615
      |	GROUP BY kostl,ktext
      |)t4
      |ON t4.kostl = t1.kostl
      |left join (
      |	SELECT prctr,ktext
      |	FROM sunac.dwd_fd_sap_hs_cepc_df
      |	WHERE stat_date = 20220615
      |	GROUP BY prctr,ktext
      |)t5
      |ON t1.prctr = t5.prctr
      |left JOIN (
      |	SELECT rstgr,rstgt,zbblx
      |	FROM sunac.dwd_fd_sap_hs_ztfi0258_df
      |	WHERE stat_date = 20220615
      |  	AND zbblx = '08'
      |  	and mandt = '800'
      |	GROUP BY rstgr,zbblx,rstgt
      |) t6
      |ON t1.zzrstgr = t6.rstgr
      |LEFT JOIN (
      |	SELECT lifnr,name1
      |	FROM sunac.dwd_fd_sap_hs_lfa1_df
      |	WHERE stat_date = 20220615
      |	GROUP BY lifnr,name1
      |) t8
      |ON t1.zzlfinr = t8.lifnr
      |
      |LEFT JOIN (
      |	SELECT kunnr,name1
      |	FROM sunac.dwd_fd_sap_hs_kna1_df
      |	WHERE stat_date = 20220615
      |	GROUP BY kunnr,name1
      |) t7
      |ON t1.zzkunnr = t7.kunnr
      |left join (
      |	SELECT belnr,bukrs,gjahr,BXDNR
      |	FROM sunac.dwd_fd_sap_hs_ztfi0361_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,BXDNR
      |) t9
      |ON t1.belnr = t9.belnr
      |AND t1.bukrs = t9.bukrs
      |AND t1.gjahr = t9.gjahr
      |left join (
      |	SELECT belnr,bukrs,gjahr,BXDNR
      |	FROM sunac.dwd_fd_sap_hs_ztfi0314_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,BXDNR
      |) t10
      |ON t1.belnr = t10.belnr
      |AND t1.bukrs = t10.bukrs
      |AND t1.gjahr = t10.gjahr
      |left join (
      |	SELECT belnr,bukrs,gjahr,BXDNR
      |	FROM sunac.dwd_fd_sap_hs_ztfi_fkconf_log_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,BXDNR
      |) t11
      |ON t1.belnr = t11.belnr
      |AND t1.bukrs = t11.bukrs
      |AND t1.gjahr = t11.gjahr
      |LEFT JOIN (
      |	SELECT belnr,bukrs,gjahr,BXDNR
      |	FROM sunac.dwd_fd_sap_hs_ztfi3015_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,BXDNR
      |)t12
      |ON t1.belnr = t12.belnr
      |AND t1.bukrs = t12.bukrs
      |AND t1.gjahr = t12.gjahr
      |LEFT JOIN (
      |	SELECT belnr,bukrs,gjahr,FKSQD_DBKEY
      |	FROM sunac.dwd_fd_sap_hs_ztfi0097_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY
      |)t13
      |ON t1.belnr = t13.belnr
      |AND t1.bukrs = t13.bukrs
      |AND t1.gjahr = t13.gjahr
      |LEFT JOIN (
      |	SELECT belnr,bukrs,gjahr,FKSQD_DBKEY
      |	FROM sunac.dwd_fd_sap_hs_ztfi3016_df
      |	WHERE stat_date = 20220615
      |	GROUP BY belnr,bukrs,gjahr,FKSQD_DBKEY
      |)t14
      |ON t1.belnr = t14.belnr
      |AND t1.bukrs = t14.bukrs
      |AND t1.gjahr = t14.gjahr
      |where t1.stat_date= 20220615
      |""".stripMargin

  var readEs : String =
    """select
      | `bkpfxref2hd`   --string COMMENT '融创凭证号',
      | ,`bkpfzyzid`   --string COMMENT '预制凭证号',
      | ,`bkpfbktxt`   --string COMMENT '凭证抬头文本',
      | ,`bsegbelnr`   --string COMMENT '会计凭证号码',
      | ,`bkpfnumpg`   --string COMMENT '附件张数',
      | ,`bkpfkursf`   --string COMMENT '汇率',
      | ,`bsegbuzei`   --string COMMENT '行项目数',
      | ,`bsegbukrs`   --string COMMENT '公司代码',
      | ,`bseggjahr`   --string COMMENT '财年',
      | ,`bsegh2monat`   --string COMMENT '会计期间',
      | ,to_date(from_unixtime(UNIX_TIMESTAMP(bsegh2bldat,'yyyyMMdd')))  as bsegh2bldat --Timestamp COMMENT '业务日期',
      | ,to_date(from_unixtime(UNIX_TIMESTAMP(bsegh2budat,'yyyyMMdd'))) as bsegh2budat--Timestamp COMMENT '记帐日期',
      | ,`bsegh2bblart`   --string COMMENT '凭证类型',
      | ,`bsegsgtxt`   --string COMMENT '摘要',
      | ,`bseghkont`   --string COMMENT '总分类帐帐目',
      | ,`ska1txt50`   --string COMMENT '总账科目长文本',
      | ,`bsegbschl`   --string COMMENT '记帐代码',
      | ,`bsegh2hwaer`   --string COMMENT '本币',
      | ,cast(`bsegs2wrbtr` as decimal(20,2))  --double COMMENT '借方金额',
      | ,`bsegh2wrbtr`   --double COMMENT '贷方金额',
      | ,`bseggsber`   --string COMMENT '业务类型',
      | ,`bsegshkzg`   --string COMMENT '借方/贷方标识',
      | ,`bsegdmbtr`   --double COMMENT '本位币金额',
      | ,`bsegkostl`   --string COMMENT '成本中心',
      | ,`bsegprctr`   --string COMMENT '利润中心',
      | ,`bsegzzrstgr`   --string COMMENT '现金流量码',
      | ,`bseghktid`   --string COMMENT '银行标识',
      | ,`bseghbkid`   --string COMMENT '开户银行',
      | ,`bsegkunnr`   --string COMMENT '客户编号',
      | ,`bseglifnr`   --string COMMENT '供应商或债权人的帐号',
      | ,`bseganln1`   --string COMMENT '主资产号',
      | ,`bseganbwa`   --string COMMENT '资产业务类型',
      | ,`bsegzzxiangmu`   --
      | ,`bsegzzheto`   --
      | ,`bsegmwskz`   --
      | ,`bsegzzbeiyong1`   --
      | ,`bsegzzbeiyong2`   --
      | ,`bsegzfbdt`   --
      | ,`bsegzzmenge`
      | ,`bsegmeins`   --
      | ,`bsegxnegp`
      | ,`bsegzzhktid`
      | ,`bsegbxdnr`
      | ,`bkpftcode`   --
      | ,`bsegzzlfinr`
      | ,`bsegzzshulz`   --
      | ,`bsegzzkunnr`   --
      | ,`bsegzzltext`   --
      | ,`bsegzzwyfwlx`
      | ,`bsegzzskxz`
      | ,`bsegzzcollc`
      | ,`bkpfxreversal`
      | ,`bkpftjusr`
      | ,`bkpftjdate`
      | ,`bkpftjtime`
      | ,`bkpfgzusr`
      | ,`bkpfgzrdt`
      | ,`bkpfgztim`
      | ,`csksktext`
      | ,`cepcktext`   --
      | ,`ztfi0258rstgt`
      | ,`kna1name1`
      | ,`lfa1name1`   --
      | from sunac.ads_fd_sap_hs_journal_di
      | where stat_date = 20220622""".stripMargin
}
