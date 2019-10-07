package com.knoldus.util

import java.io.{File, FileOutputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait HDFSConnectionFactory {
  val conf: Configuration
  val fs: FileSystem
}

object ConnectionProvider extends HDFSConnectionFactory {
  val conf = new Configuration
  System.setProperty("HADOOP_USER_NAME", ConfigConstants.hdfsUser)
  conf.set("fs.defaultFS", s"${ConfigConstants.hdfsUrl}")
  conf.set("dfs.replication", "1")
  val fs = FileSystem.get(conf)
  val key: Text = new Text()
  val value: BytesWritable = new BytesWritable()

  val path = new Path(s"${ConfigConstants.hdfsDir}${ConfigConstants.unitId}/")

  val dirPath =  s"${ConfigConstants.hdfsDir}${ConfigConstants.unitId}/"
  val imageIds: List[String] = fs.listStatus(path)
    .toList.map(_.getPath.toString)
    .map(_.replace(s"${ConfigConstants.hdfsUrl}${ConfigConstants.hdfsDir}${ConfigConstants.unitId}/",""))
  def readPathAndSaveToDir(imageUUID: String, filesSaved: Int = 0): Future[Int] = Future {
    val leftFileUrl = s"$dirPath$imageUUID/$imageUUID-L"
    val rightFileUrl = s"$dirPath$imageUUID/$imageUUID-R"
    recursiveRead(leftFileUrl, imageUUID, true)
    recursiveRead(rightFileUrl, imageUUID, false)
  }

  private def recursiveRead(path: String, UUID: String, isLeft: Boolean): Int = {
    val pathSeq = new Path(path)
    try {
      val reader: SequenceFile.Reader = new SequenceFile.Reader(fs, pathSeq, conf)
      val key: Text = new Text()
      val value: BytesWritable = new BytesWritable()
      while (reader.next(key, value)) {
        val dir = s"${ConfigConstants.destFolder}${ConfigConstants.unitId}/$UUID/${if (isLeft) "Left" else "Right"}"
        val dirFile = new File(dir)
        if (!dirFile.exists()) {
          dirFile.mkdirs()
        }
        val filePath = s"$dir/${key.toString}"
        println(filePath)
        val tosave = new File(filePath)
        val os: OutputStream = new FileOutputStream(tosave)
        os.write(value.getBytes)
        os.close()
      }
    } catch {
      case exception: Exception =>
        println(s"$exception")
    }
    0
  }
}
