package com.elex.bigdata.countuidurl;

import com.elex.bigdata.countuidurl.utils.TableStructure;
import com.elex.bigdata.util.MetricMapping;
import org.apache.commons.io.FileUtils;
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

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/2/14
 * Time: 3:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUidUrlRunner implements Runnable{
  private static Logger logger=Logger.getLogger(CountUidUrlRunner.class);
  private String project;
  private String nation;
  private String startTime,endTime;
  private String output;
  public CountUidUrlRunner(String project,String nation,String startTime,String endTime,String outputBase){
     this.project=project;
     this.nation=nation;
     this.startTime=startTime;
     this.endTime=endTime;
     this.output=outputBase+ "/"+project+"/"+nation;
  }
  @Override
  public void run() {
    byte[] startRk=Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)},Bytes.toBytes(nation),Bytes.toBytes(startTime));
    byte[] endRk=Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)},Bytes.toBytes(nation),Bytes.toBytes(endTime));
    try {
      getUrlCount(startRk,endRk,output);
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private void getUrlCount(byte[] startRk,byte[] endRk,String output) throws Exception {
    Configuration conf= HBaseConfiguration.create();
    /*
       set Job MapperClass,ReducerClass,INputFormatClass,InputPath,OutPutPath
     */
    Job job=Job.getInstance(conf);
    job.setMapperClass(GetUidUrlMap.class);
    job.setReducerClass(CountUidUrlReduce.class);
    job.setInputFormatClass(TableInputFormat.class);


    MultipleInputs.addInputPath(job, new Path("/user/hadoop/"), TableInputFormat.class, GetUidUrlMap.class);
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.setJarByClass(CountUidUrl.class);

    //set Scan and init Mapper for Hbase Table

    Scan scan=new Scan();
    scan.setStartRow(startRk);
    scan.setStopRow(endRk);
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url));
    int cacheing=1024;
    scan.setCaching(cacheing);
    logger.info("init TableMapperJob");
    TableMapReduceUtil.initTableMapperJob(TableStructure.tableName, scan, GetUidUrlMap.class, Text.class, Text.class, job);
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
