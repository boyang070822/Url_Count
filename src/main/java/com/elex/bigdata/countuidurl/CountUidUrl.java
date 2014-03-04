package com.elex.bigdata.countuidurl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.elex.bigdata.utils.ScanRangeUtil.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/26/14
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUidUrl {
  private static Logger logger=Logger.getLogger(CountUidUrl.class);
  //the job runs once per day
  public static void main(String[] args) throws Exception {
    /*input has output Path(named with day(hour(minute)))
      if has the second arg,then it is the startTime.the Time should be format of 'yyyyMMddHHmmss';
      if not then set the endTime to currentTime. and the start time should be set to scanUnit before it.
    */
    /*
      first get the length of args.
      if the length <1 or >2 then return;
      if the length =1 then set the endTime and get ScanStartTime
      else if the length=2
              get the first Char of args[1],
              if it is 's', parse to the ScanStartTime and get ScanEndTime
              else if it is 'e',parse to the ScanEndTime and getScanStartTime
    */
    if(args.length<1||args.length>2){
      logger.error("args length should be >=1 and <2 . the first outputPath ,the second startTime");
      return;
    }
    String output=args[0];
    Date startScanTime = null,endScanTime = null;
    DateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    Date scanUnitTime=getScanUnit();
    if(args.length==1){
      endScanTime=new Date();
      startScanTime=getStartScanTime(endScanTime,scanUnitTime);
    }else{
      char type=args[1].charAt(0);
      if(type=='s'){
        startScanTime=format.parse(args[1].substring(1));
        endScanTime=getEndScanTime(startScanTime,scanUnitTime);
      }
      else if(type=='e'){
        endScanTime=format.parse(args[1].substring(1));
        startScanTime=getStartScanTime(endScanTime,scanUnitTime);
      }
      else {
        logger.error("args[1] should start with 's' to indicate startTime or 'e' to indicate endTime");
      }
    }

    //get hbase Configuration
    Configuration conf= HBaseConfiguration.create();
    /*
       set Job MapperClass,ReducerClass,INputFormatClass,InputPath,OutPutPath
     */
    Job job=Job.getInstance(conf);
    job.setMapperClass(GetUidUrlMap.class);
    job.setReducerClass(CountUidUrlReduce.class);
    job.setInputFormatClass(TableInputFormat.class);

    MultipleInputs.addInputPath(job, new Path("/user/hadoop/"), TableInputFormat.class, GetUidUrlMap.class);
    FileOutputFormat.setOutputPath(job,new Path(output));
    job.setJarByClass(CountUidUrl.class);

    //set Scan and init Mapper for Hbase Table

    logger.debug("start: "+startScanTime+" end: "+endScanTime);

    Scan scan=new Scan();
    scan.setStartRow(Bytes.toBytes(format.format(startScanTime)));
    scan.setStopRow(Bytes.toBytes(format.format(endScanTime)));
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url));
    int cacheing=1024;
    scan.setCaching(cacheing);
    logger.info("init TableMapperJob");
    TableMapReduceUtil.initTableMapperJob(TableStructure.tableName,scan,GetUidUrlMap.class,Text.class,Text.class,job);
    // submit job
    logger.info("submit job");
    try{
      job.waitForCompletion(true);
    }catch (Exception e){
      e.printStackTrace();
      throw e;
    }
  }


}
