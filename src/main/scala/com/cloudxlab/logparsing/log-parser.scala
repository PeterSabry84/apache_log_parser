package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
    

    def containsURL(line:String):Boolean = return line matches "^.*\"\\S+\\s(\\S+html) .*$"
    
    
    //Extract only URLs accessing html pages to exlude .gif and other resources
    def extractURL(line:String):(String) = {
    
        val pattern = "^.*\"\\S+\\s(\\S+html) .*$".r
        val pattern(url:String) = line
        return (url.toString)
    }
    def extractHTTPcode(line:String):(String) = {
    
        val pattern = "^.*\"\\s(\\S+) .*$".r
        val pattern(httpcode:String) = line
        return (httpcode.toString)
    }
    def extractTimeFrames(line:String):(String) = {
    
        val pattern = "^.*\\s\\[(\\S+:\\d{2}):\\d{2}:\\d{2}.*$".r
        val pattern(tf:String) = line
        return (tf.toString)
    }     

    def gettop10urls(accessLogs:RDD[String], sc:SparkContext):Array[(String,Int)] = {
        //Keep only the lines which have URLs
        var URLaccessLog = accessLogs.filter(containsURL)
        var accessURLs = URLaccessLog.map(extractURL(_))
        var accessURLs_pairs = accessURLs.map((_,1));
        var frequencies = accessURLs_pairs.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(10)
    }
    def gethttpcodes(accessLogs:RDD[String], sc:SparkContext):Array[(String,Int)] = {
   
    var httpcodes = accessLogs.map(extractHTTPcode(_))
    var httpcodes_pairs = httpcodes.map((_,1));
    var frequencies = httpcodes_pairs.reduceByKey(_ + _);
    var sortedfrequencies = frequencies.sortBy(x => x._2, false)
    return sortedfrequencies.collect
    }
    def gettimeframes(accessLogs:RDD[String], sc:SparkContext, ordr:Boolean):Array[(String,Int)] = {
   
    var timeframes = accessLogs.map(extractTimeFrames(_))
    var timeframes_pairs = timeframes.map((_,1));
    var frequencies = timeframes_pairs.reduceByKey(_ + _);
    var sortedfrequencies = frequencies.sortBy(x => x._2, ordr)
    return sortedfrequencies.take(5)
    }
    
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint  /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 2) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

   
        var accessLogs = sc.textFile(args(1))
        val top10urls = utils.gettop10urls(accessLogs, sc)
        val http_codes = utils.gethttpcodes(accessLogs, sc)
        val high_timeframes = utils.gettimeframes(accessLogs, sc,false)
        val low_timeframes = utils.gettimeframes(accessLogs, sc,true)
        println("===== TOP 10 Requested URL =====")
        for(i <- top10urls){
            println(i)
        }
        println("===== TOP 5 time frames for high traffic =====")
        for(i <- high_timeframes){
            println(i)
        }
        println("===== TOP 5 time frames for least traffic =====")
        for(i <- low_timeframes){
            println(i)
        }
        println("===== HTTP Codes =====")
        for(i <- http_codes){
            println(i)
        }
    }
}
