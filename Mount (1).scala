// Databricks notebook source
val containerName = "mycontainer"
val storageAccountName = "mysaccyb"
val sas = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-05-30T02:58:09Z&st=2021-05-10T18:58:09Z&spr=https,http&sig=j2ScNS320lfeP3LNSZrYFIud8DjBMIXlRzzsD08fog4%3D"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------



// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/files",
extraConfigs = Map(config -> sas))

// COMMAND ----------

// MAGIC %fs ls /mnt/files
