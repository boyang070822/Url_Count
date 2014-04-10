package com.elex.bigdata.countuidurl;

import com.elex.bigdata.countuidurl.utils.TableStructure;
import com.elex.bigdata.util.MetricMapping;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.model.KeyRangeComparator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/2/14
 * Time: 3:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUidUrlRunner  {
  private static Logger logger = Logger.getLogger(CountUidUrlRunner.class);
  private String project;
  private List<String> nations;
  private String startTime, endTime;
  private String output;
  private Job job;

  public CountUidUrlRunner(String project, List<String> nations, String startTime, String endTime, String outputBase) {
    this.project = project;
    this.nations = nations;
    this.startTime = startTime;
    this.endTime = endTime;
    this.output = outputBase + "/" + project + "/" + startTime + "_" + endTime;
  }

  public void _run() {

    List<KeyRange> keyRangeList=new ArrayList<KeyRange>();
    for (String nation : nations ) {
      byte[] startRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(startTime));
      byte[] endRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(endTime));
      KeyRange keyRange=new KeyRange(startRk,true,endRk,false);
      keyRangeList.add(keyRange);
    }
    KeyRangeComparator comparator=new KeyRangeComparator();
    Collections.sort(keyRangeList,comparator);
    byte[] startRk=keyRangeList.get(0).getLowerRange();
    byte[] endRk=keyRangeList.get(keyRangeList.size()-1).getUpperRange();
    for(KeyRange kr: keyRangeList){
      System.out.println("keyRange: "+Bytes.toStringBinary(kr.getLowerRange())+" ----- "+Bytes.toStringBinary(kr.getUpperRange()));
    }
    try {
      getUrlCount(startRk, endRk,keyRangeList,output);
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private void getUrlCount(byte[] startRk, byte[] endRk, String output) throws Exception {
    //set Scan and init Mapper for Hbase Table

    Scan scan = new Scan();
    scan.setStartRow(startRk);
    scan.setStopRow(endRk);
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url));
    int cacheing = 1024;
    scan.setCaching(cacheing);
    logger.info("init TableMapperJob");
    getUrlCount(scan,output);

  }

  public void getUrlCount(byte[] startRk, byte[] endRk, List<KeyRange> keyRangeList,String output) throws IOException {
    Scan scan = new Scan();
    scan.setStartRow(startRk);
    scan.setStopRow(endRk);
    SkipScanFilter filter=new SkipScanFilter(keyRangeList);
    scan.setFilter(filter);
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url));
    int cacheing = 1024;
    scan.setCaching(cacheing);
    logger.info("init TableMapperJob");
    getUrlCount(scan,output);
  }

  public void getUrlCount(Scan scan,String output) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    /*
       set Job MapperClass,ReducerClass,INputFormatClass,InputPath,OutPutPath
     */
    job = Job.getInstance(conf);
    job.setMapperClass(GetUidUrlMap.class);
    job.setReducerClass(CountUidUrlReduce.class);
    job.setInputFormatClass(TableInputFormat.class);
    try{
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(new Path(output)))
      fs.delete(new Path(output));
    }catch (IOException e){
      e.printStackTrace();
    }
    MultipleInputs.addInputPath(job, new Path("/user/hadoop/"), TableInputFormat.class, GetUidUrlMap.class);
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.setJarByClass(CountUidUrl.class);
    TableMapReduceUtil.initTableMapperJob(TableStructure.tableName, scan, GetUidUrlMap.class, Text.class, Text.class, job);
    // submit job
    logger.info("submit job");
    job.setJobName("CountUidUrl");
  }

  public Job getJob(){
    _run();
    return job;
  }

}
