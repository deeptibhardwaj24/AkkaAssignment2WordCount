package com.knoldus.akka
import java.io.File
import akka.actor._
import akka.routing.RoundRobinPool
import scala.collection.mutable


/**
  * Created by knoldus on 16/3/16.
  */

object WordCount  {

  val system = ActorSystem("WordCount")
  val divider = system.actorOf(Props[Divider],"divider")
  val merger= system.actorOf(Props[Merger],"merger")
  val router: ActorRef = system.actorOf(RoundRobinPool(3).props(Props[countActor]), "router")

  def buildMap(line: String): Map[String, Int] ={
    val res=line.toString.split(" ").groupBy(a => a).map { case (a, b) => (a, b.length)}
    res
  }

  def main(args: Array[String]) {

    val file = new File("/home/knoldus/akkaassignment2/wordcountfile")
    divider ! file
  }
}

class Divider extends Actor{

  def receive = {
    case file: File => {

      for (line <- scala.io.Source.fromFile(file).getLines)
      WordCount.router ! line
    }
  }
}

class countActor extends Actor{

  def receive = {
    case line:String => {

     WordCount.merger ! WordCount.buildMap(line)
    }
  }
}

class Merger extends  Actor {

  var fmap = scala.collection.mutable.Map[String, Int]()
  def receive = {
    case myMap: Map[String, Int] =>

    fmap= fmap ++ myMap.map{ case (k,v) => k -> (v + fmap.getOrElse(k,0)) }
      println(fmap)
  }
}

