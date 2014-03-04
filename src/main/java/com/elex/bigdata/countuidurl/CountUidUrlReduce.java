package com.elex.bigdata.countuidurl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/26/14
 * Time: 6:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUidUrlReduce extends Reducer<Text,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(CountUidUrlReduce.class);
  public void reduce(Text uid,Iterable<Text> urls,Context context) throws IOException, InterruptedException {
    Map<String,Integer> map=new HashMap<String, Integer>();
    for(Text url: urls){
      Integer num=map.get(url.toString());
      if(num==null)
      {
        num=new Integer(0);
        map.put(url.toString(),num);
      }
      map.put(url.toString(),++num);
    }
    //logger.info("unit uid"+uid.toString());
    for(Map.Entry<String,Integer> entry: map.entrySet()){
      context.write(uid,new Text(entry.getKey()+"\t"+entry.getValue()));
      logger.debug("uid "+uid+" url: "+entry.getKey().toString());
    }
  }
}
