package com.research.util

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
  * Created by reja on 02/11/18.
  * this class for utilize get config file, reformat timestamp and config to database
  */
@SerialVersionUID(1L)
class AppUtil extends Serializable {

  //=== get property from config file
  def configutil(name: String, path: String): String = {
    val prop = new Properties()
    val inputStream = new FileInputStream(path)
    prop.load(inputStream)
    inputStream.close()
    prop.getProperty(name)
  }

  def timestamptodate(times: Long, format: String): String = {
    val sf = new SimpleDateFormat(format)
    val date = new Date(times)
    sf.format(date)
  }

  def hbaseconfig(path: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.master", configutil("hbaseMaster", path))
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", configutil("zkQuorum", path))
    conf.set("hbase.client.keyvalue.maxsize", "0")
    conf.set("hbase.client.scanner.timeout.period", "180000")
    conf.set("hbase.rpc.timeout", "180000")
    conf.set("mapred.output.dir", "/tmp")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
    conf.set(TableInputFormat.SCAN_CACHEDROWS, "50000")
    conf
  }

  def hbasesparkconfig(path: String, startrow: String, stoprow: String, tablename: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.master", configutil("hbaseMaster", path))
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", configutil("zkQuorum", path))
    conf.set("hbase.client.keyvalue.maxsize", "0")
    conf.set("hbase.client.scanner.timeout.period", "180000")
    conf.set("hbase.rpc.timeout", "180000")
    conf.set("mapred.output.dir", "/tmp")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
    conf.set(TableInputFormat.SCAN_ROW_START, startrow)
    conf.set(TableInputFormat.SCAN_ROW_STOP, stoprow)
    conf.set(TableInputFormat.SCAN_COLUMNS, "0")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    conf

  }

}
