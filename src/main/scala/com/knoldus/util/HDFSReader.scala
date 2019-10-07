package com.knoldus.util

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object HDFSReader extends App {
  val res = Future.sequence(ConnectionProvider.imageIds.map(imageId => {
    ConnectionProvider.readPathAndSaveToDir(imageId)
  })).map(_.sum).map { _ =>
    println("process complete")

    ConnectionProvider.fs.close()
    0
  }

  Await.ready(res, Duration.Inf)
}
