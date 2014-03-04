package com.elex.bigdata.loaduidurlcount;

import com.elex.bigdata.utils.HTableUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 5:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadUidUrlCountReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
  private static byte[] cf= Bytes.toBytes("result");
  private static byte[] count=Bytes.toBytes("count");
  private static byte[] ts=Bytes.toBytes("timestamp");
  private static int putNum=0;
  public static HTable hTable;
  public void reduce(Text uidUrl,Iterable<IntWritable> counts, Context context) throws IOException {
     Get get=new Get(Bytes.toBytes(uidUrl.toString()));
     get.addColumn(cf,count);
     Result result=hTable.get(get);
     int urlCount=0;
     for(KeyValue kv: result.raw()){
       urlCount=Bytes.toInt(kv.getValue());
       break;
     }
     for(IntWritable element:counts)
       urlCount++;
     Put put =new Put(Bytes.toBytes(uidUrl.toString()));
     put.add(cf,count,Bytes.toBytes(urlCount));
     hTable.put(put);
     putNum++;
     if(putNum== HTableUtil.putBatch)
       hTable.flushCommits();
  }
}
