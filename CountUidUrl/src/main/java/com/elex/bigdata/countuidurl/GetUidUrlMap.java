package com.elex.bigdata.countuidurl;

import com.elex.bigdata.countuidurl.utils.TableStructure;
import org.apache.hadoop.hbase.KeyValue;
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
      byte[] uid= Arrays.copyOfRange(row.get(),TableStructure.uidIndex,row.get().length);
      for(KeyValue kv: value.raw()){
        context.write(new Text(Bytes.toString(uid)),new Text(Bytes.toString(kv.getValue())));
      }
      //logger.info("Map uid "+Bytes.toString(uid)+" url "+Bytes.toString(url));
  }
}
