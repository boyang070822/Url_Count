package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/3/14
 * Time: 9:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class LDocProducer implements Runnable {
  String input, output;
  String localOutPutBase;
  String project,nation,outputTime;

  //constructor input,output
  public LDocProducer(String inputBase, String outputBase, String localOutPutBase,String project, String nation, String inputTime, String outputTime) {
    input = inputBase + File.separator + project + File.separator + nation + File.separator + inputTime;
    output = outputBase + File.separator + project + File.separator + nation + File.separator + outputTime;
    this.project=project;
    this.nation=nation;
    this.outputTime=outputTime;
    this.localOutPutBase=localOutPutBase;
  }

  @Override
  public void run() {
    Configuration conf = new Configuration();
    Job job = null;
    try {
      job = Job.getInstance(conf);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    job.setMapperClass(LabeledDocMapper.class);
    job.setReducerClass(LabeledDocReducer.class);
    job.setJarByClass(LabeledDocument.class);
    try {
      FileInputFormat.addInputPath(job, new Path(input));
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    try {
      try {
        job.waitForCompletion(true);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    try {
      Runtime.getRuntime().exec("hadoop fs -getmerge " +output+ "  "+localOutPutBase+File.separator+project+File.separator+nation+File.separator+outputTime);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}