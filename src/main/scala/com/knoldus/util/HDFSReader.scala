package com.knoldus.util

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Try


object HDFSReader extends App {

  println("===================================================\n")
  println("Press 1 to use hdfs file expander\n")
  println("Press 2 to use save to local file saver\n")
  println("===================================================\n")

  val userInput: String = Try(scala.io.StdIn.readLine()).fold(_ => "", identity)

  val res = userInput match {
    case "2" => localFileExpander
    case "1" => hdfsFileExpander
    case _ =>
      println("Unsupported Input")
      Future(0)
  }


  def localFileExpander: Future[Int] =if(ConfigConstants.destFolder == ""){
    println("Exception: Destination folder environment variable missing\n Please set it using export DESTINATION_FOLDER=\"<folder_name>\"")
    Future(0)
  } else if(ConfigConstants.imageUUID == ""){
    Future.sequence(ConnectionProvider.imageIds.map(imageId => {
      ConnectionProvider.readPathAndSaveToDir(imageId)
    })).map(_.sum).map { _ =>
      println("process complete")
      ConnectionProvider.fs.close()
      0
    }
  } else {
    ConnectionProvider.readPathAndSaveToDir(ConfigConstants.imageUUID)
  }

  @scala.annotation.tailrec
  def generateSlices(imageList: List[String], slices: List[List[String]] = List.empty[List[String]], sliceValue: Int = 4):List[List[String]] = {
    if(imageList.isEmpty) slices else {
      val (firstFour, rest) = imageList.span(imageList.indexOf(_) < 4)
      generateSlices(rest, slices ::: List(firstFour))
    }
  }


  def hdfsFileExpander: Future[Int] = {
    if(ConfigConstants.imageUUID == ""){
      println(s"Image IDs ${ConnectionProvider.imageIds.length}")

      val slices = generateSlices(ConnectionProvider.imageIds)
      processImages(slices)
//      Future.sequence(ConnectionProvider.imageIds.map(imageId => {
//        Try(ConnectionProvider.readPathAndSaveToHDFS(imageId)).fold(exception => Future(0), identity)
//      })).map(_.sum).map { _ =>
//        println("process complete")
//        ConnectionProvider.fs.close()
//        0
//      }
    } else {
      ConnectionProvider.readPathAndSaveToHDFS(ConfigConstants.imageUUID)
    }
  }

  def processImages(slices: List[List[String]], startingValue: Int = 0):Future[Int] = {
    if(slices.nonEmpty){
      println(s"=============== running for ${slices.head}")
    }
    slices match {
      case Nil =>
        Future(startingValue)
      case head :: tail =>
        Future.sequence(head.map(imageId => {
          ConnectionProvider.readPathAndSaveToHDFS(imageId).recover{
            case exception: Exception =>
              println(s"exception in hdfs $exception")
              0
          }
        })).map(_.sum).flatMap(res => processImages(tail, startingValue + res))
    }
  }

  Await.ready(res, Duration.Inf)
}
