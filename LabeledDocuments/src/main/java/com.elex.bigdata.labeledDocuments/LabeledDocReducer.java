package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocReducer extends Reducer<Text,Text,Text,Text> {
  private Map<String,String> url_categories=new HashMap<String,String>();
  private Map<String,String> category_Labels=new HashMap<String, String>();
  protected void setup(Context context) throws IOException {
    InputStream inputStream=this.getClass().getResourceAsStream("/url_category");
    BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
    String line=null;
    while((line=reader.readLine())!=null){
      String[] categoryUrls=line.split(" ");
      for(int i=1;i<categoryUrls.length;i++){
        url_categories.put(categoryUrls[i],categoryUrls[0]);
      }
    }
    reader.close();
    inputStream.close();
    InputStream labelInputStream=this.getClass().getResourceAsStream("/category_labels");
    BufferedReader labeledReader=new BufferedReader(new InputStreamReader(labelInputStream));
    String labelLine=null;
    while((labelLine=labeledReader.readLine())!=null){
      String[] categoryLabels=labelLine.split("=");
      category_Labels.put(categoryLabels[0], categoryLabels[1]);
    }
    labeledReader.close();
    labelInputStream.close();

  }
  protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
    //key is uid, value is url:cf
    //result is uid:[labels] array[url,cf"\t"]
    //label is topic id .the topic id will be return according to url
    String uid=key.toString();
    Set<String> labels=new HashSet<String>();
    StringBuilder builder=new StringBuilder();
    for(Text urlCfText:values){
      String urlCf=urlCfText.toString();
      String url=urlCf.split(",")[0];
      if(category_Labels.containsKey(url)){
        labels.add(category_Labels.get(url));
      }
      builder.append(urlCf+" ");
    }
    builder.deleteCharAt(builder.length()-1);

    StringBuilder labelsBuilder=new StringBuilder();
    labelsBuilder.append("[");
    for(String label:labels){
      labelsBuilder.append(label+",");
    }
    labelsBuilder.deleteCharAt(labelsBuilder.length()-1);
    labelsBuilder.append("]");
    context.write(new Text(uid), new Text(labelsBuilder.toString() +" "+ builder.toString()));
  }
}
