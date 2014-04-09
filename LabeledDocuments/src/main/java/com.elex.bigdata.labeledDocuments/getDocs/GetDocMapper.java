package com.elex.bigdata.labeledDocuments.getDocs;

import com.elex.bigdata.labeledDocuments.LabeledDocMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/9/14
 * Time: 5:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetDocMapper extends Mapper<Object,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(GetDocMapper.class);
  private Set<String> currentUids=new HashSet<String>();
  protected void setup(Context context) throws IOException {
     String inputPath=context.getConfiguration().get("input");
     Path path=new Path(inputPath);
     FileSystem fs=FileSystem.get(context.getConfiguration());
     if(fs.isDirectory(path)){
        FileStatus[] fileStatuses=fs.listStatus(path);
        for(FileStatus fileStatus : fileStatuses){
          if(fileStatus.isFile()){
            FSDataInputStream inputStream=fs.open(fileStatus.getPath());
            BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
            String line=null;
            while((line=reader.readLine())!=null){
               String uid=line.split("\t")[0];
               currentUids.add(uid);
            }
          }
        }
     }else {
       FSDataInputStream inputStream=fs.open(path);
       BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
       String line=null;
       while((line=reader.readLine())!=null){
         String uid=line.split("\t")[0];
         currentUids.add(uid);
       }
     }
  }
  protected void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    //get uid url cf from uidUrlCount text .
    //every line is composed of uid url and count split by "\t"
    String[] fields=value.toString().split("\t");
    if(fields.length!=3){
      logger.info("error value "+value);
      return;
    }
    if(currentUids.contains(fields[0]))
      context.write(new Text(fields[0]),new Text(fields[1]+","+fields[2]));
    else
      System.out.println(fields[0] +" not in current uids");
  }
}
