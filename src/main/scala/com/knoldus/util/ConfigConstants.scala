package com.knoldus.util

import com.typesafe.config.{Config, ConfigFactory}

object ConfigConstants {
  val config: Config = ConfigFactory.load()
  val hdfsUser = config.getString("hdfs-user")
  val hdfsUrl = config.getString("hdfs-url")
  val unitId = config.getString("unit-id")
  val hdfsDir = config.getString("hdfs-dir")
  val destFolder = config.getString("destination-folder")
}
