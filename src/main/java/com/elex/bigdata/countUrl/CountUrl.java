package com.elex.bigdata.countUrl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/26/14
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUrl {
  private static Logger logger=Logger.getLogger(CountUrl.class);
  public static void main(String[] args) throws Exception {
    String output=args[0],day=args[1];
    Configuration conf=new Configuration();
    Job job=Job.getInstance(conf);
    job.setMapperClass(GetUidUrlMap.class);
    job.setReducerClass(CountUrlReduce.class);
    job.setInputFormatClass(TableInputFormat.class);
    String nextDay=getNextDay(day);
    Scan scan=new Scan();
    scan.setStartRow(Bytes.toBytes(day));
    scan.setStopRow(Bytes.toBytes(nextDay));
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url));
    logger.info("init TableMapperJob");
    TableMapReduceUtil.initTableMapperJob(TableStructure.tableName,scan,GetUidUrlMap.class,Text.class,Text.class,job);
    MultipleInputs.addInputPath(job, new Path("/user/hadoop/"), TableInputFormat.class, GetUidUrlMap.class);
    FileOutputFormat.setOutputPath(job,new Path(output));
    job.setJarByClass(CountUrl.class);
    logger.info("submit job");
    try{
      job.waitForCompletion(true);
    }catch (Exception e){
      e.printStackTrace();
      throw e;
    }
  }
  public static String getNextDay(String day) throws ParseException {
    DateFormat format=new SimpleDateFormat("yyyyMMdd");
    Date date=format.parse(day);
    date.setDate(date.getDate()+1);
    return format.format(date);
  }
}
