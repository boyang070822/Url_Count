package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocument {
  public static void main(String[] args) throws IOException {
    /*
      args[0] inputPath args[1] outputPath

    */
    String input=args[0],output=args[1];
    //set mapper,reducer,inputPath,outputPath and so on
    Configuration conf=new Configuration();
    Job job=Job.getInstance(conf);
    job.setMapperClass(LabeledDocMapper.class);
    job.setReducerClass(LabeledDocReducer.class);
    job.setJarByClass(LabeledDocument.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.setMapOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
