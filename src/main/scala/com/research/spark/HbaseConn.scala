package com.research.spark

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.research.util.AppUtil
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, _}
import org.apache.hadoop.hbase.util.Bytes
import org.bson.Document

import scala.collection.JavaConversions._


/**
  * Created by reja on 08/11/18.
  * this class for query data graph from hbase via client mode
  */
@SerialVersionUID(1L)
class HbaseConn extends Serializable {

  def getListdata(path: String, source: String, critId: String, sdate: String, edate: String): Seq[Document] = {

    //====== initialize params
    val st = System.currentTimeMillis()
    val config = new AppUtil()
    //    val critId = "7a67431d3a5bf94861ff29428683ac14"
    //    var source = "twitter"
    var types: String = ""
    source match {
      case "twitter" =>
        types = "TW"
      case "facebook" =>
        types = "FB"
      case "instagram" =>
        types = "IG"
    }

    //======= reformat date
    //    val startTimeStr = config.timestamptodate("1541203200000".toLong,"yyyyMMdd")
    //    val endTimeStr = config.timestamptodate("1541773498000".toLong,"yyyyMMdd")
    val startTimeStr = config.timestamptodate(sdate.toLong, "yyyyMMdd")
    val endTimeStr = config.timestamptodate(edate.toLong, "yyyyMMdd")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val start = dateFormat.parse(startTimeStr)
    val end = dateFormat.parse(endTimeStr)
    println(start)
    println(end)
    val startCal: Calendar = Calendar.getInstance
    startCal.setTime(start)
    val endCal: Calendar = Calendar.getInstance
    endCal.setTime(end)
    import java.util.Calendar
    endCal.add(Calendar.DATE, 1)


    //======= connect to hbase
    val con: Connection = ConnectionFactory.createConnection(config.hbaseconfig(path))
    var hbaseTable: Table = null
    var listData: Seq[Document] = Seq[Document]()
    try {
      hbaseTable = con.getTable(TableName.valueOf("sna-graph"))
      val listGets = new util.ArrayList[Get]
      while (startCal.before(endCal)) {
        println("=== get list id ====")
        val rowId = critId + "_|_" + types + "_|_" + dateFormat.format(startCal.getTime)
        listGets.add(new Get(Bytes.toBytes(rowId)))
        startCal.add(Calendar.DATE, 1)
        println(rowId)
      }

      val results = hbaseTable.get(listGets)
      for (result <- results) {
        println("loop === data")
        val listCell = result.listCells
        if (listCell != null && listCell.size > 0) {
          for (cell <- listCell) {
            val docData = Document.parse(Bytes.toString(result.getValue(Bytes.toBytes("0"), CellUtil.cloneQualifier(cell))))
            listData :+= docData
          }
        }
      }
    } catch {
      case e: Exception => println("\t=== > " + e.printStackTrace())
    }

    val et = System.currentTimeMillis() - st
    println(et)

    listData
  }

}
