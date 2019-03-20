package com.research.spark

import better.files._
import com.research.model.GraphMapping
import com.research.util.AppUtil
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
//import com.ebdesk.sna.module.twitter.topic.TwitterTopicOverallAnalisis
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.bson.Document

import scala.util.control.Breaks

/**
  * Created by reja on 08/11/18.
  * This class for generate data mongo into file .gexf via graph and spark sql
  */
@SerialVersionUID(1L)
class SparkGephiHbase extends Serializable {

  def run(path: String, source: String, criteria: String, sdate: String, edate: String, properties: String): Unit = {
    case class User(name: String, types: String, deg: Int)

    //======= start process time
    val st = System.currentTimeMillis()

    //======= initialize spark
    val spark = SparkSession.builder()
//            .master("local[4]")
      .appName("SparkHbaseGephi")
      .getOrCreate()

    //======== set SparkContext
    val sc = spark.sparkContext

    //======= set log level spark
    sc.setLogLevel("ERROR")

    //====== initialize params
    val config = new AppUtil()
    var types: String = ""
    source match {
      case "twitter" =>
        types = "TW"
      case "facebook" =>
        types = "FB"
      case "instagram" =>
        types = "IG"
    }
    val startTimeStr = config.timestamptodate(sdate.toLong, "yyyyMMdd")
    val endTimeStr = config.timestamptodate(edate.toLong, "yyyyMMdd")

    //====== load data from hbase and filtering data
    println(criteria + "_|_" + types + "_|_" + startTimeStr)
    println(criteria + "_|_" + types + "_|_" + endTimeStr)
    val conf = new AppUtil().hbasesparkconfig(properties, criteria + "_|_" + types + "_|_" + startTimeStr,
      criteria + "_|_" + types + "_|_" + endTimeStr, "sna-graph")
    val hBaseRDD: RDD[Document] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).flatMap(result => {
      val listcell = result._2.listCells()
      var docs: Document = null
      import java.util
      val listdoc = new util.ArrayList[Document]
      for (cell <- listcell) {
        val tmp = Bytes.toString(result._2.getValue(Bytes.toBytes("0"), CellUtil.cloneQualifier(cell)))
        docs = Document.parse(tmp)
        listdoc.add(docs)
      }
      listdoc.iterator
    })

    //========== process time load data from hbase
    val tq = System.currentTimeMillis() - st
    println(tq)
    //    hBaseRDD.foreach(println(_))

    //    val hbasedata = new HbaseConn()
    //    var HbaseRDD: RDD[Document] = sc.parallelize(hbasedata.getListdata(properties, source, criteria, sdate, edate))

    //====== map data from mongo to rdd & dataset
    val RDDnode = hBaseRDD.filter(x => {
      var flags = false
      if (x.get("target") != null) {
        flags = true
      }
      flags
    }).map {
      x =>
        var sourceid: Long = 0
        var targetid: Long = 0
        var sourcename: String = ""
        var targetname: String = ""
        var sourcetype: String = ""
        var targettype: String = ""
        if (x.containsKey("target") && x.getString("target_type").equals("TW_Account")) {
          targetid = x.getString("target").replace("TW_", "").toLong
          targetname = x.getString("target_name")
          targettype = x.getString("target_type")
        }
        if (x.containsKey("source") && x.getString("source_type").equals("TW_Account")) {
          sourceid = x.getString("source").replace("TW_", "").toLong
          sourcename = x.getString("source_name")
          sourcetype = x.getString("source_type")
        }
        if (x.containsKey("target") && !x.getString("target_type").equals("TW_Account")) {
          targetid = 0
          targetname = x.getString("target_name")
          targettype = x.getString("target_type")
        }
        if (x.containsKey("source") && !x.getString("source_type").equals("TW_Account")) {
          sourceid = 0
          sourcename = x.getString("source_name")
          sourcetype = x.getString("source_type")
        }
        (sourceid, targetid, sourcename, targetname, sourcetype, targettype, x.getString("type"), x.getString("text"),
          x.getLong("timestamp"), x.getString("post_id"))
    }
    val df = spark.createDataFrame(RDDnode).toDF("sourceid", "targetid", "sourcename", "targetname", "sourcetype",
      "targettype", "typeedges", "text", "timestamp", "post_id")
    //    df.printSchema()

    val dftarget = df.select("targetid", "targetname", "targettype").where("targettype != ''")
    val dfsource = df.select("sourceid", "sourcename", "sourcetype").where("sourcetype != ''")
    val dftmp = dfsource.union(dftarget).dropDuplicates("sourcename")
      .withColumn("row_id", row_number().over(Window.orderBy("sourceid")))
    //    dftmp.show(500)

    //====== makes rdd nodes
    //    var temp: List[(Long, (String, String))] = List()
    val vRDD2: RDD[(VertexId, (String, String))] =
    dftmp.select("sourceid", "sourcename", "sourcetype", "row_id")
      .rdd
      .map(x => {
        var sourceid: Long = 0
        var rowid: Long = 0
        var sourcename: String = ""
        var sourcetype: String = ""
        if (x.get(0) != 0) {
          sourceid = x.getLong(0)
          sourcename = x.getString(1)
          sourcetype = x.getString(2)
        } else {
          sourceid = x.getInt(3).toLong
          sourcename = x.getString(1)
          sourcetype = x.getString(2)
        }
        (sourceid, (sourcename, sourcetype))
      })

    //====== make rdd edges
    val eRDD2: RDD[Edge[(String, String, String, String, Long, String, String, String)]] = df
      .select("sourceid", "targetid", "typeedges", "text", "sourcename", "targetname", "timestamp", "post_id",
        "sourcetype", "targettype")
      .rdd
      .map({ row =>
        Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue(),
          (row(2).asInstanceOf[String], row(3).asInstanceOf[String], row(4).asInstanceOf[String],
            row(5).asInstanceOf[String], row(6).asInstanceOf[Long], row(7).asInstanceOf[String],
            row(8).asInstanceOf[String], row(9).asInstanceOf[String]))
      })

    //====== default for missing nodes
    val defaultUser = ("name", "Missing")

    //======= making graph from rdd nodes and rdd edges
    val graph: Graph[(String, String), (String, String, String, String, Long, String, String, String)] =
      Graph(vRDD2, eRDD2, defaultUser)


    //====== filter missing node
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    //==== Create a user Graph with mapping degrees property
    val initialUserGraph: Graph[User, (String, String, String, String, Long, String, String, String)] =
      validGraph.mapVertices {
        case (id, (name, types)) => User(name, types, 0)
      }

    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.degrees) {
      case (id, u, degOpt) => User(u.name, u.types, degOpt.getOrElse(0))
    }

    //======= mapping data graphx to gephi
    val gephi = new GraphMapping()
    gephi.init()

    //====== filtering node base degree and count nodes
    val countnode = userGraph.vertices.repartition(300).map(f => {
      (f._2.name.toString, 1)
    }).reduceByKey((x, y) => {
      x + y
    }).map(_._2).sum()
    println("node graphx : " + countnode)
    val loop = new Breaks

    var graphfilterdegree: Array[(Long, User)] = Array()
    var nodedegree: RDD[(VertexId, (String, String))] = null
    if (countnode > 15000) {
      loop.breakable {
        for (i <- 0 to countnode.toInt) {
          println("=======process filtering adaptive=====")
          //======= process filtering adaptive
          val graphfilter = userGraph.vertices.filter {
            case (id, u) => (u.deg) >= i
          }.repartition(300).map(f => {
            (f._2.name.toString, 1)
          }).reduceByKey((x, y) => {
            x + y
          }).map(_._2).sum()

          if (graphfilter <= 15000) {
            println("value - " + i)
            //            graphfilterdegree = userGraph.vertices.filter {
            //              case (id, u) => (u.inDeg + u.outDeg) >= i
            //            }.collect
            nodedegree = userGraph.vertices.filter {
              case (id, u) => u.deg >= i
            }.map { x => (x._1, (x._2.name.toString, x._2.types.toString)) }
            loop.break
          }
        }
      }
    } else {
      //      graphfilterdegree = userGraph.vertices.filter {
      //        case (id, u) => (u.inDeg + u.outDeg) > 0
      //      }.collect
      nodedegree = userGraph.vertices.filter {
        case (id, u) => u.deg > 0
      }.map { x => (x._1, (x._2.name.toString, x._2.types.toString)) }
    }

    //====== create graph from filtering nodes
    val graphnone: Graph[(String, String), (String, String, String, String, Long, String, String, String)] =
      Graph(nodedegree, userGraph.edges, defaultUser)
    val graphgephi = graphnone.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    //    val edgefilter = graphgephi.edges.collect
    //    val nodefilter = graphgephi.vertices.collect

    //====== create nodes
    graphgephi.vertices.collect.foreach(f => {
      val node = gephi.createNode(f._1.toString, f._2._1.toString, f._2._2.toString)
      gephi.directAddNode(node)
    })

    //====== create edges
    graphgephi.edges.collect.foreach(y => {
      val nodetarget = gephi.createNode(y.dstId.toString, y.attr._4.toString, y.attr._8.toString)
      val targetnode = gephi.directAddNode(nodetarget)
      val nodesource = gephi.createNode(y.srcId.toString, y.attr._3.toString, y.attr._7.toString)
      val targetsource = gephi.directAddNode(nodesource)
      val edge = gephi.createEdge(y.attr._1.toString, y.attr._2.toString, y.attr._6.toString, y.attr._5,
        y.attr._3.toString, y.attr._4.toString, y.srcId, y.dstId, targetsource, targetnode)
      gephi.directAddEdge(edge)
    })

    //====== export to gephi file
    println("direct graph node : " + gephi.getDirectGraphs.getNodeCount + " ====== edges : " + gephi.getDirectGraphs.getEdgeCount)

    //======= create folder
    val dir: File = path
      .toFile
      .createIfNotExists(true)
    gephi.exportGephi("default", path + "/")

    //====== analytic process
    //    val twitterTopicOverallAnalisis =
    //      new TwitterTopicOverallAnalisis(gephi.getDirectGraphs.getNodeCount, gephi.getDirectGraphs.getEdgeCount)
    //    val filename = path + "/twitter_topic_overall.gexf"
    //    val result = twitterTopicOverallAnalisis.runAnalisis(path + "/default.gexf", filename, true, 500.0)

    val et = System.currentTimeMillis() - st
    println("========= time process generate : " + et)

    sc.stop()
  }

}
