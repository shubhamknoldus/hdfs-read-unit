package com.knoldus.util

import java.io.{File, FileOutputStream, OutputStream}
import java.util

import com.knoldus.util.ConfigConstants._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Try

/**
 * Custom execution context of 50 threads maximum of 40 files can be processed parallelly
 */
object ContextProvider {
  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(new java.util.concurrent.ForkJoinPool(50))

  def shutdownPool(): util.List[Runnable] = ec.shutdownNow()
}

trait HDFSConnectionFactory {
  val conf: Configuration
  val fs: FileSystem

  def closeFS: Unit = fs.close()
}

/**
 * custome mode
 * @param fileName sequence file name
 * @param folderName one level up sub directory
 * @param sequenceFilePath absolute path of the sequence file
 * @param pathOnLocal path to be saved under
 */
case class HDFSPath(fileName: String, folderName: String, sequenceFilePath: String, pathOnLocal: String = "")

object HDFSPath {
  def apply(fileStatus: FileStatus): HDFSPath = {
    val pathSplitted = fileStatus.getPath.toString.split("/")
    if (pathSplitted.length >= 6) HDFSPath(pathSplitted(pathSplitted.length - 1),
      pathSplitted(pathSplitted.length - 2), fileStatus.getPath.toString)
    else HDFSPath(pathSplitted(4), "", fileStatus.getPath.toString)
  }
}

object ConnectionProvider extends HDFSConnectionFactory {

  import ContextProvider._

  val conf = new Configuration
  System.setProperty("HADOOP_USER_NAME", ConfigConstants.hdfsUser)
  conf.set("fs.defaultFS", s"${ConfigConstants.hdfsUrl}")
  conf.set("dfs.replication", "1")
  val fs: FileSystem = FileSystem.get(conf)
  val key: Text = new Text()
  val value: BytesWritable = new BytesWritable()

  val path: Path = new Path(s"${ConfigConstants.hdfsDir}")
  val filesAtPath: Vector[HDFSPath] = {
    val (files, dirs) = fs.listStatus(path).toVector.partition(_.isFile)
    getAllFilesWithFromTheDirectory(files, dirs).map(HDFSPath.apply)
  }



  def files: Array[FileStatus] = fs.listStatus(path)

  def saveTheFiles: Future[Int] = {
    val listOfFiles: List[HDFSPath] =  createDirs.values.flatten.toList
    val slices: List[List[HDFSPath]] = generateSlices[HDFSPath](listOfFiles)
    processSlices(slices)
  }

 private def processSlices(slices: List[List[HDFSPath]], startingValue: Int = 0): Future[Int] = {

    slices match {
      case Nil =>
        println(s"Processed $startingValue slices")
        Future{startingValue}
      case head :: tail =>
        Future.sequence(head.map(counterSlice => {
          recursiveRead(counterSlice).recover{
            case exception: Exception =>
              println(s"Exception $exception in processing counter slice $counterSlice")
              0
          }
        })).map(_.sum).flatMap(_ => processSlices(tail, startingValue + 1))
    }
  }

  private def createDirs: Map[String, Vector[HDFSPath]] = {
    val grouped = filesAtPath.groupBy(_.folderName)
    println(s"Going to create ${grouped.size} directories")
    val dirs = grouped.map {
      case (str, files) =>
        val parent = new File(s"$destFolder${str}")
        parent.mkdir()
        val finalFiles = files.filter(hdPath => checkIfSequenceFile(hdPath.sequenceFilePath)).map(value => {
          val localPath = s"${parent.getAbsolutePath}/${value.fileName}"
          val fileInner = new File(s"${parent.getAbsolutePath}/${value.fileName}")
          fileInner.mkdir()
          value.copy(pathOnLocal = localPath)
        })
        (str, finalFiles)
    }
    dirs
  }

  private def recursiveRead(file: HDFSPath): Future[Int] = Future  {
      val pathSeq = new Path(file.sequenceFilePath)
      val reader: SequenceFile.Reader = new SequenceFile.Reader(fs, pathSeq, conf)
      try {
        val key: Text = new Text()
        val value: BytesWritable = new BytesWritable()
        while (reader.next(key, value)) {
          val dir = file.pathOnLocal
          val dirFile = new File(dir)
          if (!dirFile.exists()) {
            dirFile.mkdirs()
          }
          val filePath = s"$dir/${key.toString}"
          val tosave = new File(filePath)
          val os: OutputStream = new FileOutputStream(tosave)
          os.write(value.getBytes)
          os.close()
        }
        reader.close()
      } catch {
        case exception: Exception =>
          println(s"$exception")
          reader.close()
      }
    0
  }

  @scala.annotation.tailrec
  private def getAllFilesWithFromTheDirectory(files: Vector[FileStatus], dirs: Vector[FileStatus]): Vector[FileStatus] = {
    val (file, dir) = dirs.foldLeft((files, Vector.empty[FileStatus]))({
      case ((fileIn, dirIn), fileStatus) =>
        val (fileTes, dirTes) = fs.listStatus(fileStatus.getPath).partition(_.isFile)
        (fileIn ++ fileTes.toVector, dirTes.toVector ++ dirIn)
    })

    if (dir.isEmpty) file
    else getAllFilesWithFromTheDirectory(file, dir)
  }

  @scala.annotation.tailrec
  private def generateSlices[T](imageList: List[T], slices: List[List[T]] = List.empty[List[T]], sliceValue: Int = 40): List[List[T]] = {
    if (imageList.isEmpty) slices else {
      val (firstFour, rest) = imageList.span(imageList.indexOf(_) < 40)
      generateSlices(rest, slices ::: List(firstFour))
    }
  }

  private def checkIfSequenceFile(path: String): Boolean = {
    val filePath = fs.open(new Path(path))
    Try{
      val bA = new Array[Byte](4)
      filePath.read(bA,0,4)
      val baStr = new String(bA)
      if(baStr.contains("SEQ")) {
        Try(bA(3).toChar.toInt).fold({
          _ =>
            false
        }, {value =>
          value > 0 && value <= 6
        })
      } else false
    }.fold({ex =>
      println(s"Exception $ex while checking file $path")
      filePath.close()
      false
    }, { value =>
      filePath.close()
      value
    })
  }
}
