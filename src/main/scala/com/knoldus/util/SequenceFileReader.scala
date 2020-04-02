package com.knoldus.util

import java.io.{File, FileOutputStream, OutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait HDFSConnectionFactory {
  val conf: Configuration
  val fs: FileSystem
}

object ConnectionProvider extends HDFSConnectionFactory {
  val localConf = new Configuration
  System.setProperty("HADOOP_USER_NAME", "freaks")
  localConf.set("fs.defaultFS", "hdfs://localhost:4569")
  localConf.set("dfs.replication", "1")
  val localFs = FileSystem.get(localConf)




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

  def readPathAndSaveToHDFS(imageUUID: String, filesSaved: Int = 0): Future[Int] = Future {
    println(s"Running for imageIds $imageUUID")
    val leftFileUrl = s"$dirPath$imageUUID/$imageUUID-L"
    val rightFileUrl = s"$dirPath$imageUUID/$imageUUID-R"
    recursiveReadHDFS(leftFileUrl, imageUUID, true)
    recursiveReadHDFS(rightFileUrl, imageUUID, false)
  }


  private def saveTempFile(fileBytes: Array[Byte], filePath: String) : (String, String) = {
    val path = new Path(filePath)
    val file: FSDataOutputStream = localFs.create(path)
    try {
      file.write(fileBytes)
    } catch {
      case exception: Exception =>
        println("Failed to save file in HDFS", exception)
    } finally {
      try {
        file.close()
      } catch {
        case exception: Exception =>
          println("Exception occurred in save temp file")
      } finally {

      }
    }
    (filePath, dirPath)
  }

  private def recursiveReadHDFS(path: String, UUID: String, isLeft: Boolean): Int = {
    val pathSeq = new Path(path)
    val reader: SequenceFile.Reader = new SequenceFile.Reader(fs, pathSeq, conf)
    try {
      val key: Text = new Text()
      val value: BytesWritable = new BytesWritable()
      while (reader.next(key, value)) {
        val dir = s"${ConfigConstants.tempImageDir}${ConfigConstants.unitId}/$UUID/${if (isLeft) "Left" else "Right"}"
        val filePath = s"$dir/${key.toString}"
        saveTempFile(value.getBytes, filePath)
      }
    } catch {
      case exception: Exception =>
        println(s"Exception in recursiveReadHDFS $exception")
        reader.close()
    }
    0
  }

  private def recursiveRead(path: String, UUID: String, isLeft: Boolean): Int = {
    val pathSeq = new Path(path)
    val reader: SequenceFile.Reader = new SequenceFile.Reader(fs, pathSeq, conf)
    try {
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
        reader.close()
    }
    0
  }
}
