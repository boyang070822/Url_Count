package com.elex.bigdata.loaduidurlcount;


import com.elex.bigdata.utils.HTableUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 4:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadUidUrlCount {
  /*
     accumulate the uid:url-count key-value
     the data stored into htable 'uidUrlCount_22find'
      get the sum and increase by count
  */
  public static void main(String[] args) throws IOException {
    // input is args[0],which actual is the path of countuidurl outPut path.
    // output is args[1] which is the name of htable.
    String input=args[0];
    String tableName=args[1];

    Configuration conf= HBaseConfiguration.create();
    Job job=Job.getInstance(conf);

    job.setMapperClass(LoadUidUrlCountMapper.class);
    job.setReducerClass(LoadUidUrlCountReducer.class);
    FileInputFormat.addInputPath(job, new Path(input));
    LoadUidUrlCountReducer.hTable=new HTable(conf,tableName);
    LoadUidUrlCountReducer.hTable.setAutoFlush(false);
    LoadUidUrlCountReducer.hTable.setScannerCaching(HTableUtil.caching);
    TableMapReduceUtil.initTableReducerJob(tableName,LoadUidUrlCountReducer.class,job);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    LoadUidUrlCountReducer.hTable.flushCommits();
  }

}
