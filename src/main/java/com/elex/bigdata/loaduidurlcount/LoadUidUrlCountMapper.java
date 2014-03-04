package com.elex.bigdata.loaduidurlcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 5:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadUidUrlCountMapper extends Mapper<Object,Text,Text,IntWritable> {
  private static Logger logger=Logger.getLogger(LoadUidUrlCountMapper.class);
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    String[] fields=value.toString().split("\t");
    //for(int i=0;i<fields.length;i++)
    //  logger.debug("field "+i+": "+fields[i]);
    context.write(new Text(fields[1]+fields[2]),null);
  }
}
