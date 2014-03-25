package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocMapper extends Mapper<Object,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(LabeledDocMapper.class);
  protected void map(Object key,Text value,Context context) throws IOException, InterruptedException {
     //get uid url cf from uidUrlCount text .
     //every line is composed of uid url and count split by "\t"
     String[] fields=value.toString().split("\t");
     if(fields.length!=3){
       logger.info("error value "+value);
       return;
     }
     context.write(new Text(fields[0]),new Text(fields[1]+","+fields[2]));
  }
}
