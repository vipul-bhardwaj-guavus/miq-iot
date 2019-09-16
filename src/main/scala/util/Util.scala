package com.guavus.vzb.util

import java.io.{BufferedInputStream, FileInputStream, IOException, InputStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object Util {

  def getInputStream(str: String, isHDFS: Boolean = true) : InputStream = {
    if(isHDFS){
      val conf = new Configuration
      val fs = FileSystem.get(conf)
      val path = new Path(str)
      fs.open(path)
    }
    else {
      new BufferedInputStream(new FileInputStream(str))
    }
  }

  def loadProperties(configFile: String, isHDFS: Boolean = true): Properties = {

    println(s"Loading properties file : ${configFile}")

    val prop = new Properties()
    try {
      val input = getInputStream(configFile, isHDFS)
      prop.load(input)
      if (input != null) {
        input.close()
      }
    } catch {
      case ex: IOException => ex.printStackTrace()
    }
    prop
  }

}
