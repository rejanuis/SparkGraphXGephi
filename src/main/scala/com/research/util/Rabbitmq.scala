package com.research.util

import java.util.{HashMap, Map}

import com.rabbitmq.client.ConnectionFactory

/**
  * Created by reja on 05/11/18.
  * this class for consumer and produce data to rabbit
  */
@SerialVersionUID(1L)
class Rabbitmq extends Serializable{

  //====== function to send daa to rabbit
  def producerabbit(path: String, jsondata : String): Unit = {
    val util = new AppUtil()
    val connFactory = new ConnectionFactory()
    connFactory.setUsername(util.configutil("user", path))
    connFactory.setPassword(util.configutil("password", path))
    connFactory.setHost(util.configutil("host", path))
    connFactory.setPort(5672)

    import com.rabbitmq.client.AMQP
    val connection = connFactory.newConnection()
    val channel = connection.createChannel()
    // channel.queueDeclare(apps.getProperty(path, "producer_queue"), false,
    // false, false, null);

    val props = new AMQP.BasicProperties.Builder()
    val header: Map[String, Object] = new HashMap[String, Object]()
    header.put("x-delay", "0")
    props.headers(header)
    channel.basicPublish(util.configutil("producer_queue", path), util.configutil("routingkeyProd", path), null, jsondata.toString.getBytes("UTF-8"))
    println(" Sending data '" + jsondata.toString + "'")

    connection.close()
  }

}
