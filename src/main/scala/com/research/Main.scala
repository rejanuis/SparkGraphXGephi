package com.research

/**
  * Created by reja on 02/11/18.
  * This main class for get data mongo or hbase & generate gephi
  */

import java.nio.charset.StandardCharsets

import com.rabbitmq.client.AMQP._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope}
import org.json.JSONObject


object Main {
  def main(args: Array[String]): Unit = {

    //===  create connection to rabbitmq
    val paths = args(0)
    val modedb = args(1)
    //    val paths = "/home/reja/workspace_AP/generator-csv-network/config.properties"
    //    val modedb = "hbase"
    val util = new AppUtil()
    val rabbit = new Rabbitmq()
    val connFactory = new ConnectionFactory()
    connFactory.setUsername(util.configutil("user", paths))
    connFactory.setPassword(util.configutil("password", paths))
    connFactory.setHost(util.configutil("host", paths))
    connFactory.setPort(5672)
    val connection = connFactory.newConnection()
    val channel = connection.createChannel()

    //===== declare class spark
    val gephisparkmongo = new SparkGephiMongo()
    val gephisparkhbase = new SparkGephiHbase()
    //==== limit 1 message each consume from rabbit
    channel.basicQos(1)

    //==== consume data from rabbitmq
    println(" [*] Waiting for messages. please be waiting :)")
    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope,
                                  properties: BasicProperties, body: Array[Byte]) {
        val st = System.currentTimeMillis()
        val str = new String(body, StandardCharsets.UTF_8)
        println("received: " + str)
        try {
          val jsons = new JSONObject(str)
          println("received: " + jsons.getString("path"))
          //======= case which database want to query
          modedb match {
            case "mongo" => {
              gephisparkmongo.run(jsons.getString("path"), jsons.getString("platform"), jsons.getString("criteria_id"),
                jsons.getString("from"), jsons.getString("to"), paths)
            }
            case "hbase" => {
              gephisparkhbase.run(jsons.getString("path"), jsons.getString("platform"), jsons.getString("criteria_id"),
                jsons.getString("from"), jsons.getString("to"), paths)
            }
          }

          //========= send data json for generate analytic gephi
          rabbit.producerabbit(paths, jsons.put("step", "analysis_network").toString)
        } catch {
          case e: Exception => println("\t=== > " + e.printStackTrace())
        } finally {
          println("======= processed done ======");
          channel.basicAck(envelope.getDeliveryTag(), false);
          val done = System.currentTimeMillis() - st
          println("======== total process : " + done)
        }


      }
    }

    channel.basicConsume(util.configutil("consumer_queue", paths), false, consumer)

  }
}
