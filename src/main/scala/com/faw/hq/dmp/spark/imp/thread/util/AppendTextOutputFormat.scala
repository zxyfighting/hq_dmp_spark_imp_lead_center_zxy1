package com.faw.hq.dmp.spark.imp.thread.util

import java.io.DataOutputStream
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.{Progressable, ReflectionUtils}

/**
* auth:张秀云
* purpose:让文件滚动追加
*/
  class AppendTextOutputFormat extends TextOutputFormat[Any, Any] {
    override def getRecordWriter(ignored: FileSystem, job: JobConf, iname: String, progress: Progressable): RecordWriter[Any, Any] = {
      val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
      val keyValueSeparator: String = job.get("mapreduce.output.textoutputformat.separator", "\t")
      //自定义输出文件名
      val currentTime: Date = new Date()
      //val formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
      val formatter = new SimpleDateFormat("yyyy-MM-dd");
      val dateString = formatter.format(currentTime);
      val name = job.get(dateString, iname)
      if (!isCompressed) {
        val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
        val fs: FileSystem = file.getFileSystem(job)
        val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name)
        //如果获得路径已经存在，就追加
        val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
          fs.append(newFile)
        } else {
          fs.create(file, progress)
        }
        new TextOutputFormat.LineRecordWriter[Any, Any](fileOut, keyValueSeparator)
      } else {
        val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
        // create the named codec
        val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
        // build the filename including the extension
        val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
        val fs: FileSystem = file.getFileSystem(job)
        val newFile: Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)
        //如果获得路径已经存在，就追加
        val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
          fs.append(newFile)
        } else {
          fs.create(file, progress)
        }
        new TextOutputFormat.LineRecordWriter[Any, Any](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
      }
    }


}