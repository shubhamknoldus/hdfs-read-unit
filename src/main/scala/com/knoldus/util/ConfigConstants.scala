package com.knoldus.util

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Just to load env vars from environment
 */
object ConfigConstants {
  val help =
    """
      |1) A valid hadoop user name shall be exported in th environment using "export HDFS_USER=hadoop"
      |2) A valid hadoop path shall be provided which has sequence file you can provide top most directory
      |   using "export HDFS_DIR=<directory containing sequence files>
      |3) A valid destination folder on local filesystem shall be provided using "export DESTINATION_FOLDER="destfolder/" and it must end with a "/"
      |""".stripMargin
  val config: Config = ConfigFactory.load()
  val hdfsUser: String = config.getString("hdfs-user")
  val hdfsUrl: String = config.getString("hdfs-url")
  val hdfsDir: String = config.getString("hdfs-dir")
  val destFolder: String = config.getString("destination-folder")
  if(hdfsUser == ""||
      hdfsDir == "" ||
    destFolder == "" || ConfigConstants.destFolder(destFolder.length - 1) != '/'){
    println(help)
    sys.exit(1)
  }
}
