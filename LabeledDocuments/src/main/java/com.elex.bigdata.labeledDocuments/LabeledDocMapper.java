package com.elex.bigdata.labeledDocuments;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocMapper extends Mapper<Object,Text,Text,Text> {
  private static Logger logger=Logger.getLogger(LabeledDocMapper.class);
  protected void map(Object key,Text value,Context context){
     //get uid url cf from uidUrlCount text .
     //every line is composed of uid url and count split by "\t"
  }
}
