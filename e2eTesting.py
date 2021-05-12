# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_td_dep_mast1").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_td_dep_mast")

# COMMAND ----------

display(spark.sql("select count(*) from stg_fcr_fcrlive_1_td_dep_mast"))

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_hdm_vw_ci_custmast").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_hdm_vw_ci_custmast")

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_ch_acct_cust_xref").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_ch_acct_cust_xref")

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_ba_prod_prodtype_xref").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_ba_prod_prodtype_xref")

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_ba_ccy_code").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_ba_ccy_code")

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_ba_cc_brn_mast").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_ba_cc_brn_mast")

# COMMAND ----------

spark.readStream.format("delta").load("/mnt/files/stg_fcr_fcrlive_1_ba_acct_status").withColumn("time", F.current_timestamp()).withWatermark("time", "1 minutes").createOrReplaceTempView("stg_fcr_fcrlive_1_ba_acct_status")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._

# COMMAND ----------

# MAGIC %scala
# MAGIC val df=spark.sql(""" select a.cod_acct_no,a.bal_principal,(a.rat_dep_int + a.rat_prod_var + a.rat_dep_int_var) as ROI,a.cod_dep_no,a.dat_dep_date,a.dat_maturity,a.bal_principal_ytd,a.CTR_DEP_TERM,b.cod_cust_id,c.cod_acct_cust_rel,d.cod_prod_desc,d.cod_prod,d.prod_type_desc,e.nam_ccy_short,e.nam_currency,f.nam_branch,f.cod_cc_brn 
# MAGIC from stg_fcr_fcrlive_1_td_dep_mast a 
# MAGIC left join stg_fcr_hdm_vw_ci_custmast b on a.cod_cust=b.cod_cust_id and a.time>=b.time + interval 10 minutes
# MAGIC left join stg_fcr_fcrlive_1_ch_acct_cust_xref c on trim(a.cod_acct_no) = trim(c.cod_acct_no) and a.time>=c.time + interval 10 minutes
# MAGIC left join stg_fcr_fcrlive_1_ba_prod_prodtype_xref d on a.cod_prod = d.cod_prod and a.time>=d.time + interval 10 minutes
# MAGIC left join stg_fcr_fcrlive_1_ba_ccy_code e on a.cod_ccy = e.nam_ccy_short and a.time>=e.time + interval 10 minutes
# MAGIC left join stg_fcr_fcrlive_1_ba_cc_brn_mast f on a.cod_cc_brn = f.cod_cc_brn and a.time>=f.time + interval 10 minutes
# MAGIC left join stg_fcr_fcrlive_1_ba_acct_status g on a.cod_dep_stat = g.txt_acct_status and a.time>=g.time + interval 10 minutes
# MAGIC """).withColumn("time", current_timestamp()).withWatermark("time", "10 minutes")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.writeStream.format("org.elasticsearch.spark.sql")
# MAGIC   .option("es.net.http.auth.user" ,"elastic")
# MAGIC   .option("es.net.http.auth.pass","avgSQZ2tDMS3rvQHETY1ZehX")
# MAGIC   .option("es.nodes.wan.only","true")
# MAGIC   .option("es.net.ssl","true")
# MAGIC   .option("es.nodes", "https://enterprise-search-deployment-111819.es.centralus.azure.elastic-cloud.com:9243")
# MAGIC   .outputMode("Append")
# MAGIC   .option("checkpointLocation", "/chk-point-dir3")
# MAGIC   .start("index_new")
