package com.research.spark

import com.mongodb.casbah.Imports._
import org.bson.Document

/**
  * Created by reja on 06/11/18.
  * This class for query data graph from hbase via client mode
  */
class MongoConn {

//  def main(args: Array[String]): Unit = {
  def ConnMongo() : Unit = {
    val st = System.currentTimeMillis()
    //======= connect mongodb
//    val server = new ServerAddress("localhost", 27017)
    val uri = MongoClientURI("mongodb://mongonode01:27017,mongonode02:27017,mongonode03:27017/?readPreference=secondaryPreferred")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("sna-twitter")
    val coll = db("twitter-graph")
    val start = "1540986730000"
    val end = "1541591530000"
    val criteria = MongoDBObject("criteria_id" -> "7a67431d3a5bf94861ff29428683ac14")
    val checkCount = coll.find($and(criteria)).limit(10)
    var arrayRDD: Seq[Document] = Seq[Document]()
    while(checkCount.hasNext){
      var strs: Document = Document.parse(checkCount.next().toString)
      arrayRDD :+= strs
    }
    println(arrayRDD.length)
    val done = System.currentTimeMillis() - st
    println(done)

  }

}
