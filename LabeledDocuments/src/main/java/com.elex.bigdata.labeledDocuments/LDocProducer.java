package com.elex.bigdata.labeledDocuments;

import com.elex.bigdata.labeledDocuments.inputformat.CombineTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class LDocProducer  implements Runnable{
  String input, output;
  String localOutPutBase;
  String project,outputTime;
  private Job job;

  //constructor input,output
  public LDocProducer(String inputBase, String outputBase, String localOutPutBase,String project,  String inputTime, String outputTime,boolean useProject) {
    input = inputBase + File.separator + (useProject?(project  + File.separator):"") + inputTime;
    output = outputBase + File.separator + project + File.separator + outputTime;
    this.project=project;
    this.outputTime=outputTime;
    this.localOutPutBase=localOutPutBase;
  }


  public void run() {
    Configuration conf = new Configuration();
    conf.setLong("mapred.max.split.size", 22485760); // 10m
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize",22485760);
    try {
      FileSystem fs= FileSystem.get(conf);
      if(fs.exists(new Path(output)))
        fs.delete(new Path(output));
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    job = null;
    try {
      job = Job.getInstance(conf,"getDocs");
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    job.setMapperClass(LabeledDocMapper.class);
    job.setReducerClass(LabeledDocReducer.class);
    job.setJarByClass(LabeledDocument.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    try {
      FileInputFormat.addInputPath(job, new Path(input));
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
  }
  public Job getJob(){
     run();
    return job;
  }
  public static class CopyLDocsToLocal implements Runnable{
    String output;
    String localOutPutBase;
    String project,outputTime;
    public CopyLDocsToLocal( String outputBase, String localOutPutBase,String project, String outputTime) {
      output = outputBase + File.separator + project + File.separator + outputTime;
      this.project=project;
      this.outputTime=outputTime;
      this.localOutPutBase=localOutPutBase;
    }
    @Override
    public void run() {
      copyToLocal();
    }
    public void copyToLocal(){
      try {
        File localDocDir=new File(localOutPutBase+File.separator+project);
        if(localDocDir.exists())
          localDocDir.mkdirs();
        Runtime.getRuntime().exec("hadoop fs -getmerge " +output+ "  "+localDocDir.getAbsolutePath()+File.separator+outputTime);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

}
