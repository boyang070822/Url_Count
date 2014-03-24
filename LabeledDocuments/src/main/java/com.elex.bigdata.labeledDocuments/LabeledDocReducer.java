package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocReducer extends Reducer<Text,Text,Text,Text> {
  protected void reduce(Text key,Iterable<Text> values,Context context){
    //key is uid, value is url:cf
    //result is uid:[labels] array[url,cf"\t"]
    //label is topic id .the topic id will be return according to url
  }
}
