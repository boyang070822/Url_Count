package com.elex.bigdata.loaduidurl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 5:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadUidUrlCountMapper extends Mapper<Object,Text,Text,NullWritable> {
  private static Logger logger=Logger.getLogger(LoadUidUrlCountMapper.class);
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    String[] fields=value.toString().split("\t");
    //for(int i=0;i<fields.length;i++)
    //  logger.debug("field "+i+": "+fields[i]);
    //logger.info(fields[0]+fields[1]);
    context.write(new Text(fields[0]+fields[1]),NullWritable.get());
  }
}
