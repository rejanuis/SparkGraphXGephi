package com.research.spark

/**
  * Created by reja on 25/10/18.
  * This class for generate data mongo into file .gexf via graph and spark sql
  */


import better.files._
import com.mongodb.casbah.Imports._
import com.research.model.GraphMapping
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.bson.Document

import scala.util.control.Breaks

@SerialVersionUID(1L)
class SparkGephiMongo extends Serializable {

  def run(path: String, source: String, criteria: String, sdate: String, edate: String, properties : String): Unit = {
    case class User(name: String, types: String, inDeg: Int, outDeg: Int)

    //======= initialize spark
    val spark = SparkSession.builder()
      .appName("SparkMongoGephi")
      //      .config("spark.mongodb.input.uri", "mongodb://sna:Rahas!asna20!8@192.168.114.81/temporaryDb.graph?authSource=admin&readPreference=primaryPreferred")
      //      .config("spark.mongodb.output.uri", "mongodb://sna:Rahas!asna20!8@192.168.114.81/temporaryDb.graph?authSource=admin&readPreference=primaryPreferred")
      .getOrCreate()

    //======== set SparkContext
    val sc = spark.sparkContext

    //======= set log level spark
    sc.setLogLevel("ERROR")

    //====== load data from mongo and filtering data
    //        val rdd = MongoSpark.load(sc)
    //        val aggregatedRdd : RDD[Document] = rdd.withPipeline(Seq(Document.parse("{$match : { criteria_id : '"+criteria+"' } }")))
    //        println(aggregatedRdd.count())

    val st = System.currentTimeMillis()
    val uri = MongoClientURI("mongodb://mongonode01:27017,mongonode02:27017,mongonode03:27017/?readPreference=secondaryPreferred")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("sna-twitter")
    val coll = db("twitter-graph")
    val start: String = sdate
    val end: String = edate
    val criterias = MongoDBObject("criteria_id" -> criteria)
    val checkCount = coll.find($and("timestamp" $gte start.toLong $lte end.toLong, criterias)).batchSize(100)

    var arrayRDD: Seq[Document] = Seq[Document]()

    //========== save to temp list
    while (checkCount.hasNext) {
      var strs: Document = Document.parse(checkCount.next().toString)
      arrayRDD :+= strs
    }
    println(arrayRDD.length)
    val aggregatedRdd: RDD[Document] = sc.parallelize(arrayRDD)

    //====== map data from mongo to rdd & dataset
    val RDDnode = aggregatedRdd.map {
      x =>

        var sourceid: Long = 0
        var targetid: Long = 0
        var sourcename: String = ""
        var targetname: String = ""
        var sourcetype: String = ""
        var targettype: String = ""
        if (x.containsKey("target") && x.get("target") != null && x.getString("target_type").equals("TW_Account")) {
          targetid = x.getString("target").replace("TW_", "").toLong
          targetname = x.getString("target_name")
          targettype = x.getString("target_type")
        }
        if (x.containsKey("source") && x.get("source") != null && x.getString("source_type").equals("TW_Account")) {
          sourceid = x.getString("source").replace("TW_", "").toLong
          sourcename = x.getString("source_name")
          sourcetype = x.getString("source_type")
        }
        if (x.containsKey("target") && x.get("target") != null && !x.getString("target_type").equals("TW_Account")) {
          targetid = 0
          targetname = x.getString("target_name")
          targettype = x.getString("target_type")
        }
        if (x.containsKey("source") && x.get("source") != null && !x.getString("source_type").equals("TW_Account")) {
          sourceid = 0
          sourcename = x.getString("source_name")
          sourcetype = x.getString("source_type")
        }
        (sourceid, targetid, sourcename, targetname, sourcetype, targettype, x.getString("type"), x.getString("text"),
          x.getLong("timestamp"), x.getString("post_id"))
    }
    val df = spark.createDataFrame(RDDnode).toDF("sourceid", "targetid", "sourcename", "targetname", "sourcetype",
      "targettype", "typeedges", "text", "timestamp", "post_id")
    //    df.show()

    val dftarget = df.select("targetid", "targetname", "targettype").where("targettype != ''")
    val dfsource = df.select("sourceid", "sourcename", "sourcetype").where("sourcetype != ''")
    val dftmp = dfsource.union(dftarget).dropDuplicates("sourcename")
      .withColumn("row_id", row_number().over(Window.orderBy("sourceid")))
    //    dftmp.show(500)

    //====== makes rdd nodes
    var temp: List[(Long, (String, String))] = List()
    val vRDD2: RDD[(VertexId, (String, String))] =
      dftmp.select("sourceid", "sourcename", "sourcetype", "row_id")
        //      df.select("sourceid", "targetid", "sourcename", "targetname", "sourcetype",
        //        "targettype")
        .rdd
        .flatMap(x => {
          Iterator(
            if (true) {
              if (x.get(0) != 0) {
                temp = temp :+ (x.getLong(0), (x.getString(1), x.getString(2)))
              }
              if (x.get(0) == 0) {
                temp = temp :+ (x.getInt(3).toLong, (x.getString(1), x.getString(2)))
              }
              //            if (x.get(0) != 0) {
              //              temp = temp :+ (x.getLong(0), (x.getString(2), x.getString(4)))
              //            }
              //            if (x.get(1) != 0) {
              //              temp = temp :+ (x.getLong(1), (x.getString(3), x.getString(5)))
              //            }
            }
          )
          temp
        })
        .map(row => {
          (row._1, row._2)
        })
//    vRDD2.take(1)

    //====== make rdd edges
    val eRDD2: RDD[Edge[(String, String, String, String, Long, String, String, String)]] = df
      .select("sourceid", "targetid", "typeedges", "text", "sourcename", "targetname", "timestamp", "post_id",
        "sourcetype", "targettype")
      .rdd
      .map { row =>
        Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue(),
          (row(2).asInstanceOf[String], row(3).asInstanceOf[String], row(4).asInstanceOf[String],
            row(5).asInstanceOf[String], row(6).asInstanceOf[Long], row(7).asInstanceOf[String],
            row(8).asInstanceOf[String], row(9).asInstanceOf[String]))
      }
//    eRDD2.take(2)

    //====== default for missing nodes
    val defaultUser = ("name", "Missing")

    //======= making graph from rdd nodes and rdd edges
    val graph: Graph[(String, String), (String, String, String, String, Long, String, String, String)] =
      Graph(vRDD2, eRDD2, defaultUser)
    //    val graphs = Graph(vRDD, eRDD, defaultUser)
    //    graph.cache()

    //    graph.vertices.collect.foreach(println)
    //    graph.edges.collect.foreach(println)
    //    graph.triplets.map(
    //      triplet => triplet.srcAttr._2 + " is the " + triplet.attr + " of " + triplet.dstAttr._2
    //    ).collect.foreach(println(_))
    //    val ccGraph = graph.connectedComponents()

    //====== filter missing node
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    //    validGraph.vertices.collect.foreach(println(_))
    //        validGraph.edges.collect.foreach(println(_))
    //    validGraph.triplets.map(
    //      triplet => triplet.srcAttr._2 + " is the " + triplet.attr. + " of " + triplet.dstAttr._2
    //    ).collect.foreach(println(_))

    //==== function to filer degree
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    //==== Create a user Graph with mapping degrees property
    val initialUserGraph: Graph[User, (String, String, String, String, Long, String, String, String)] =
      validGraph.mapVertices {
        case (id, (name, types)) => User(name, types, 0, 0)
      }
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.types, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.types, u.inDeg, outDegOpt.getOrElse(0))
    }

    //====== filtering node base degree and count nodes
    val countnode = userGraph.vertices.count()
    val loop = new Breaks
    var graphfilterdegree: Array[(Long, User)] = Array()
    var nodedegree: RDD[(VertexId, (String, String))] = null
    if (countnode > 15000) {
      loop.breakable {
        for (i <- 0 to countnode.toInt) {
          val graphfilter = userGraph.vertices.filter {
            case (id, u) => (u.inDeg + u.outDeg) >= i
          }.count()
          if (graphfilter <= 15000) {
            println("value - " + i)
            graphfilterdegree = userGraph.vertices.filter {
              case (id, u) => (u.inDeg + u.outDeg) >= i
            }.collect
            nodedegree = userGraph.vertices.filter {
              case (id, u) => (u.inDeg + u.outDeg) >= i
            }.map { x => (x._1, (x._2.name.toString, x._2.types.toString)) }
            loop.break
          }
        }
      }
    } else {
      graphfilterdegree = userGraph.vertices.filter {
        case (id, u) => (u.inDeg + u.outDeg) > 0
      }.collect
      nodedegree = userGraph.vertices.filter {
        case (id, u) => (u.inDeg + u.outDeg) > 0
      }.map { x => (x._1, (x._2.name.toString, x._2.types.toString)) }
    }

    //====== create graph from filtering nodes
    val graphgephi: Graph[(String, String), (String, String, String, String, Long, String, String, String)] =
      Graph(nodedegree, userGraph.edges, defaultUser)
    val edgefilter = graphgephi.edges.collect
    //    val nodefilter = graphgephi.vertices.collect

    //======= mapping data graphx to gephi
    var gephi = new GraphMapping()
    gephi.init()

    //====== create nodes
    for (f <- graphfilterdegree) {
      val node = gephi.createNode(f._1.toString, f._2.name.toString, f._2.types.toString)
      gephi.directAddNode(node)
    }

    //====== create edges
    for (y <- edgefilter) {
      val nodetarget = gephi.createNode(y.dstId.toString, y.attr._4.toString, y.attr._8.toString)
      val targetnode = gephi.directAddNode(nodetarget)
      val nodesource = gephi.createNode(y.srcId.toString, y.attr._3.toString, y.attr._7.toString)
      val targetsource = gephi.directAddNode(nodesource)
      val edge = gephi.createEdge(y.attr._1.toString, y.attr._2.toString, y.attr._6.toString, y.attr._5,
        y.attr._3.toString, y.attr._4.toString, y.srcId, y.dstId, targetsource, targetnode)
      gephi.directAddEdge(edge)
    }

    //====== export to gephi file
    println("direct graph node : " + gephi.getDirectGraphs.getNodeCount + " ====== edges : " + gephi.getDirectGraphs.getEdgeCount)

    //======= create folder
    val dir: File = path
      .toFile
      .createIfNotExists(true)
    gephi.exportGephi(criteria, path)

    sc.stop()
  }

}
