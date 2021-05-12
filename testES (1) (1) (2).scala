// Databricks notebook source
// MAGIC %scala
// MAGIC import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
// MAGIC import org.apache.kafka.common.serialization.StringSerializer
// MAGIC import java.util.Properties
// MAGIC 
// MAGIC val kafkaProducerProps: Properties = {
// MAGIC     val props = new Properties()
// MAGIC     props.put("bootstrap.servers", "10.5.0.13:9092,10.5.0.12:9092,10.5.0.14:9092,10.5.0.7:9092")
// MAGIC     props.put("key.serializer", classOf[StringSerializer].getName)
// MAGIC     props.put("value.serializer", classOf[StringSerializer].getName)
// MAGIC     props
// MAGIC   }
// MAGIC val keyMessage1="This is sample message1"
// MAGIC val keyMessage2="This is sample message2"
// MAGIC val producer = new KafkaProducer[String, String](kafkaProducerProps)
// MAGIC producer.send(new ProducerRecord[String, String]("testtopic", keyMessage1, keyMessage2))

// COMMAND ----------

// MAGIC %sh
// MAGIC pip install kafka-python

// COMMAND ----------

// MAGIC %python
// MAGIC from kafka.admin import KafkaAdminClient, NewTopic
// MAGIC admin_client = KafkaAdminClient(bootstrap_servers=['10.5.0.10','10.5.0.9','10.5.0.8'])
// MAGIC 
// MAGIC topic_list = []
// MAGIC topic_list.append(NewTopic(name="tp_test", num_partitions=1, replication_factor=1))
// MAGIC admin_client.create_topics(new_topics=topic_list, validate_only=False)

// COMMAND ----------

// MAGIC %python
// MAGIC from kafka import KafkaProducer
// MAGIC producer = KafkaProducer(bootstrap_servers=['10.5.0.8','10.5.0.9','10.5.0.10'])
// MAGIC producer.send('tp_test', b'4888607,1595688903258,STR2629,POS172')

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC val rawDF = spark.readStream.format("kafka")
// MAGIC .option("kafka.bootstrap.servers", "10.5.0.13:9092,10.5.0.12:9092,10.5.0.14:9092,10.5.0.7:9092")
// MAGIC .option("subscribe", "testtopic").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("json")
// MAGIC .queryName("Flattened Invoice Writer").outputMode("append").option("path", "/mnt")
// MAGIC .option("checkpointLocation", "/chk-point-dir").trigger(Trigger.ProcessingTime("1 minute")).start()

// COMMAND ----------

// MAGIC %sh
// MAGIC nc -vz i-o-optimized-deployment-90fad9.es.centralus.azure.elastic-cloud.com 9243

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt")

// COMMAND ----------

// MAGIC %fs ls
// MAGIC /mnt/file/Delta

// COMMAND ----------

spark.readStream.format("delta")
  .load("/mnt/file/Delta").writeStream.format("org.elasticsearch.spark.sql")
  .option("es.net.http.auth.user" ,"elastic")
  .option("es.net.http.auth.pass","zV5aazN7px7R5jzQZtN7hzs5")
  .option("es.nodes.wan.only","true")
  .option("es.net.ssl","true")
  .option("es.nodes", "https://enterprise-search-deployment-48f3e1.es.us-west1.gcp.cloud.es.io:9243")
  .outputMode("Append")
  .option("checkpointLocation", "/chk-point-diry")
  .start("index_sal")

// COMMAND ----------

// MAGIC %python
// MAGIC df=spark.sql("select Id,Address,Name,amount from sample_csv")

// COMMAND ----------

// MAGIC %python
// MAGIC df.write.format("com.databricks.spark.csv").save("/mnt/sample")

// COMMAND ----------

// MAGIC %sh 
// MAGIC curl -XDELETE 'https://i-o-optimized-deployment-90fad9.es.centralus.azure.elastic-cloud.com:9243/index'?

// COMMAND ----------

// MAGIC %scala
// MAGIC val esURL = "https://elastic:BBZOoqioNBZFpUqePKHEv0f8@i-o-optimized-deployment-90fad9.es.centralus.azure.elastic-cloud.com:9243"

// COMMAND ----------

// MAGIC %fs ls
// MAGIC /mnt

// COMMAND ----------

// MAGIC %fs ls
// MAGIC /chk-point-dir/offsets/0

// COMMAND ----------

val df = spark.read.json("/mnt/part-00000-d020caa2-3cec-4900-84ff-bae518368d4b-c000.json")

// COMMAND ----------

df.show()

// COMMAND ----------

import org.elasticsearch.spark.sql._

// COMMAND ----------

df.write
  .format("org.elasticsearch.spark.sql")
  .option("es.net.http.auth.user" ,"elastic")
  .option("es.net.http.auth.pass","zV5aazN7px7R5jzQZtN7hzs5")
  .option("es.nodes.wan.only","true")
  .option("es.net.ssl","true")
  .option("es.nodes", "https://enterprise-search-deployment-48f3e1.es.us-west1.gcp.cloud.es.io:9243")
  .mode("Overwrite")
  .save("index_new")

// COMMAND ----------

val reader = spark.read
  .format("org.elasticsearch.spark.sql")
  .option("es.net.http.auth.user" ,"elastic")
  .option("es.net.http.auth.pass","avgSQZ2tDMS3rvQHETY1ZehX")
  .option("es.nodes.wan.only","true")
  .option("es.net.ssl","true")
  .option("es.nodes", "https://enterprise-search-deployment-111819.es.centralus.azure.elastic-cloud.com:9243")

val df = reader.load("index_sal")
display(df)

// COMMAND ----------

// MAGIC %sh
// MAGIC curl -H 'Content-Type: application/json' -X GET https://elastic:avgSQZ2tDMS3rvQHETY1ZehX@enterprise-search-deployment-111819.es.centralus.azure.elastic-cloud.com:9243/index_new1/_search?pretty

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.cod_acct_no,a.bal_principal,(a.rat_dep_int + a.rat_prod_var + a.rat_dep_int_var) as ROI,a.cod_dep_no,a.dat_dep_date,a.dat_maturity,a.bal_principal_ytd,a.CTR_DEP_TERM,b.cod_cust_id
// MAGIC ,c.cod_acct_cust_rel
// MAGIC ,d.cod_prod_desc,d.cod_prod,d.prod_type_desc,e.nam_ccy_short,e.nam_currency,f.nam_branch,f.cod_cc_brn 
// MAGIC from delta.`/mnt/files/stg_fcr_fcrlive_1_td_dep_mast1` a 
// MAGIC inner join delta.`/mnt/files/stg_fcr_hdm_vw_ci_custmast` b on a.cod_cust=b.cod_cust_id 
// MAGIC inner join delta.`/mnt/files/stg_fcr_fcrlive_1_ch_acct_cust_xref` c on a.cod_acct_no = c.cod_acct_no
// MAGIC inner join delta.`/mnt/files/stg_fcr_fcrlive_1_ba_prod_prodtype_xref` d on a.cod_prod = d.cod_prod 
// MAGIC inner join delta.`/mnt/files/stg_fcr_fcrlive_1_ba_ccy_code` e on a.cod_ccy = e.nam_ccy_short 
// MAGIC inner join delta.`/mnt/files/stg_fcr_fcrlive_1_ba_cc_brn_mast` f on a.cod_cc_brn = f.cod_cc_brn 
// MAGIC inner join delta.`/mnt/files/stg_fcr_fcrlive_1_ba_acct_status` g on a.cod_dep_stat = g.txt_acct_status 
