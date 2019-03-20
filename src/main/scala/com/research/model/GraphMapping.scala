package com.research.model

import java.io.{File, IOException}

import org.gephi.graph.api._
import org.gephi.io.exporter.api.ExportController
import org.gephi.io.exporter.spi.GraphExporter
import org.gephi.project.api.{ProjectController, Workspace}
import org.openide.util.Lookup

/**
  * Created by reja on 25/10/18.
  * this class for library tools gephi, create node, edges, workspace, graph and generate files
  */

class GraphMapping extends Serializable {
  var graphModel: GraphModel = null
  var workspace: Workspace = null
  var directedGraph: DirectedGraph = null
  var exportController: ExportController = null

  def init(): Unit = {
    val pc = Lookup.getDefault.lookup(classOf[ProjectController])
    pc.newProject()
    this.workspace = pc.getCurrentWorkspace
    this.graphModel = Lookup.getDefault.lookup(classOf[GraphController]).getGraphModel(workspace)
    this.directedGraph = graphModel.getDirectedGraph()
    this.exportController = Lookup.getDefault.lookup(classOf[ExportController])
    createSchema()
  }

  def createSchema(): Unit = {
    graphModel.getNodeTable.addColumn("nodes_type", classOf[String])
    graphModel.getEdgeTable.addColumn("edges_kind", classOf[String])
    graphModel.getEdgeTable.addColumn("post_id", classOf[String])
    graphModel.getEdgeTable.addColumn("text", classOf[String])
    graphModel.getEdgeTable.addColumn("timestamp", classOf[Long])
    graphModel.getEdgeTable.addColumn("source_name", classOf[String])
    graphModel.getEdgeTable.addColumn("target_name", classOf[String])
    graphModel.getEdgeTable.addColumn("source_id", classOf[Long])
    graphModel.getEdgeTable.addColumn("target_id", classOf[Long])
  }

  def createNode(id: String, label: String, types: String): Node = {
    val node = graphModel.factory().newNode(id)
    node.setLabel(label)
    node.setAttribute("nodes_type", types)
    node
  }

  def createEdge(types: String, text: String, postId: String, timestamp: Long,
                 sourcename: String, targetname: String, sourceid: Long, targetid: Long,
                 src: Node, trg: Node): Edge = {
    val edge = graphModel.factory().newEdge(src, trg, 0, 1.0, true)
    edge.setAttribute("text", text)
    edge.setAttribute("post_id", postId)
    edge.setAttribute("edges_kind", types)
    edge.setAttribute("timestamp", timestamp)
    edge.setAttribute("source_name", sourcename)
    edge.setAttribute("target_name", targetname)
    edge.setAttribute("source_id", sourceid)
    edge.setAttribute("target_id", targetid)
    edge
  }

  def getWorkspace(): Workspace = {
    println("tes workspace " + this.workspace)
    this.workspace
  }

  def getGraphmodel(): GraphModel = {
    graphModel
  }

  def getDirectGraphs(): DirectedGraph = {
    directedGraph
  }

  def directAddNode(node: Node): Node = {
    var n = node
    try {
      directedGraph.addNode(node)
    } catch {
      case e: Exception => {
        n = directedGraph.getNode(node.getId)
      }
    }
    n
  }

  def directAddEdge(edge: Edge): Unit = {
    try {
      directedGraph.addEdge(edge)
    } catch {
      case e: Exception =>
        println("\t=== > " + e.printStackTrace())
        //        e = directedGraph.getEdge(edge.getId)

    }

  }

  def exportGephi(name: String, path: String): Unit = {

    //Export only visible graph
    val exporter = exportController.getExporter("gexf").asInstanceOf[GraphExporter] //Get GEXF exporter
    exporter.setExportVisible(true) //Only exports the visible (filtered) graph
    exporter.setWorkspace(workspace)
    try
      exportController.exportFile(new File(path + name + ".gexf"), exporter)
    catch {
      case ex: IOException =>
        ex.printStackTrace()
        return
    }
  }

}
