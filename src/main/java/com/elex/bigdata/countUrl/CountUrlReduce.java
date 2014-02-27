package com.elex.bigdata.countUrl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
public class CountUrlReduce extends Reducer<Text,Text,Text,Text> {

  public void reduce(Text uid,Iterable<Text> urls,Context context) throws IOException, InterruptedException {
    Map<Text,Integer> map=new HashMap<Text, Integer>();
    for(Text url: urls){
      Integer num=map.get(url);
      if(num==null)
      {
        num=new Integer(0);
        map.put(url,num);
      }
      map.put(url,++num);
    }
    for(Map.Entry<Text,Integer> entry: map.entrySet()){
      context.write(uid,new Text(entry.getKey()+"\t"+entry.getValue()));
    }
  }
}
