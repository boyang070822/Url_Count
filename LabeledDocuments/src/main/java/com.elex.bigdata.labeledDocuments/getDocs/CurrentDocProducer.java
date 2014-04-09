package com.elex.bigdata.labeledDocuments.getDocs;

import com.elex.bigdata.labeledDocuments.LabeledDocMapper;
import com.elex.bigdata.labeledDocuments.LabeledDocReducer;
import com.elex.bigdata.labeledDocuments.LabeledDocument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/9/14
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class CurrentDocProducer implements Runnable{
  String input, output,refInput;
  String localOutPutBase;
  String project,outputTime;

  //constructor input,output
  public CurrentDocProducer(String inputBase, String outputBase, String localOutPutBase,String project, String refInputTime,String inputTime, String outputTime,boolean useProject) {
    input = inputBase + File.separator + (useProject?(project  + File.separator):"") + inputTime;
    output = outputBase + File.separator + project + File.separator + outputTime;
    refInput= inputBase + File.separator + (useProject?(project  + File.separator):"") + refInputTime;
    this.project=project;
    this.outputTime=outputTime;
    this.localOutPutBase=localOutPutBase;
  }
  @Override
  public void run() {

    Configuration conf = new Configuration();
    conf.set("input",input);
    List<Path> refInputPaths=new ArrayList<Path>();
    Path inputPath=new Path(input);
    try {
      FileSystem fs= FileSystem.get(conf);
      if(fs.exists(new Path(output)))
        fs.delete(new Path(output));
      for(FileStatus fileStatus:fs.globStatus(new Path(output))){
         if(!fileStatus.getPath().getName().equals(inputPath.getName()))
           refInputPaths.add(fileStatus.getPath());
      }
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    Job job = null;
    try {
      job = Job.getInstance(conf);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    job.setMapperClass(GetDocMapper.class);
    job.setReducerClass(LabeledDocReducer.class);
    job.setJarByClass(CurrentDocProducer.class);
    try {
      FileInputFormat.addInputPath(job, inputPath);
      for(Path refInputPath : refInputPaths){
        FileInputFormat.addInputPath(job,refInputPath);
      }
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
    System.out.println("LDocProducer job completed");
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
