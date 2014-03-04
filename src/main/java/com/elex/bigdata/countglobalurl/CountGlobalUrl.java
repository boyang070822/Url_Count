package com.elex.bigdata.countglobalurl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 3:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountGlobalUrl {
  /*
     accumulate global url count
      get url-count key-value map from file in hadoop
      add the count for each url to get the global count
     in the future, access the flag for CountUidUrl to indicate if it has finished.

   */
  //the job runs once per day
  public static void main(String[] args) throws IOException {
    String input=args[0],output=args[1];
    //set mapper,reducer,inputPath,outputPath and so on
    Configuration conf=new Configuration();
    Job job=Job.getInstance(conf);
    job.setMapperClass(CountGlobalUrlMap.class);
    job.setReducerClass(CountGlobalUrlReduce.class);
    job.setJarByClass(CountGlobalUrl.class);
    TextInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job,new Path(output));
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
