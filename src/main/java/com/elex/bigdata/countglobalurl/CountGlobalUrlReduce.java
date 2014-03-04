package com.elex.bigdata.countglobalurl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountGlobalUrlReduce extends Reducer<Text,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(CountGlobalUrlReduce.class);
  public void reduce(Text url,Iterable<Text> counts,Context context) throws IOException, InterruptedException {
     int sum=0;
     for(Text count: counts){
       Integer num=Integer.parseInt(count.toString());
       sum+=num;
     }
     context.write(url,new Text(String.valueOf(sum)));
  }
}
