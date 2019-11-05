package com.knoldus.util

import com.typesafe.config.{Config, ConfigFactory}

object ConfigConstants {
  val config: Config = ConfigFactory.load()
  val hdfsUser: String = config.getString("hdfs-user")
  val hdfsUrl: String = config.getString("hdfs-url")
  val unitId: String = config.getString("unit-id")
  val hdfsDir: String = config.getString("hdfs-dir")
  val destFolder: String = config.getString("destination-folder")
  val imageUUID: String = config.getString("image-uuid")
  val tempImageDir: String = config.getString("hdfs-temp-dir")
}
