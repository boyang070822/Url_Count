package com.elex.bigdata.countglobalurl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 3:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountGlobalUrlMap extends Mapper<Object,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(CountGlobalUrlMap.class);
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
     //get the url and count
     String[] fields=value.toString().split("\t");
     for(int i=0;i<fields.length;i++)
       logger.info("field "+i+": "+fields[i]);
     context.write(new Text(fields[1]),new Text(fields[2]));
  }

}
