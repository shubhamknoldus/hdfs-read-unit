package com.knoldus.util

import java.time.Instant

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Main to run the application
 *
 */

object HDFSReader extends App {

  def localFileExpander: Future[Int] = {
    ConnectionProvider.saveTheFiles
  }


  println(s"Process started at  ${Instant.now()}")
  Await.ready(localFileExpander, Duration.Inf)
  ContextProvider.shutdownPool()
  ConnectionProvider.closeFS
  println(s"Process ended at  ${Instant.now()}")
}
