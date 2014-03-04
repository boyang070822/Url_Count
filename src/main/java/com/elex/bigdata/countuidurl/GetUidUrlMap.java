package com.elex.bigdata.countuidurl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/26/14
 * Time: 6:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetUidUrlMap extends TableMapper<Text,Text> {
  private static Logger logger=Logger.getLogger(GetUidUrlMap.class);
  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
      byte[] uid= Arrays.copyOfRange(row.get(),16,row.get().length);
      byte[] url= value.getValue(Bytes.toBytes(TableStructure.families[0]),Bytes.toBytes(TableStructure.url));
      logger.debug("Map uid "+Bytes.toString(uid)+" url "+Bytes.toString(url));
      context.write(new Text(Bytes.toString(uid)),new Text(Bytes.toString(url)));
  }
}
