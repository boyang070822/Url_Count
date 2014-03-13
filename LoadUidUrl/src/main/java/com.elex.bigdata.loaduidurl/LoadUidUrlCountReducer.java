package com.elex.bigdata.loaduidurl;

import com.elex.bigdata.loaduidurl.utils.HTableUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 5:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadUidUrlCountReducer extends TableReducer<Text,NullWritable,ImmutableBytesWritable> {
  private static byte[] cf= Bytes.toBytes("result");
  private static byte[] count=Bytes.toBytes("count");
  private static byte[] ts=Bytes.toBytes("timestamp");
  private static int putNum=0;
  public static HTable hTable;
  private static Logger logger=Logger.getLogger(LoadUidUrlCountReducer.class);
  public void reduce(Text uidUrl,Iterable<NullWritable> counts, Context context) throws IOException {
     Get get=new Get(Bytes.toBytes(uidUrl.toString()));
     get.addColumn(cf,count);
     Result result=hTable.get(get);
     int urlCount=0;
     if(result!=null)
     for(KeyValue kv: result.raw()){
       urlCount=Bytes.toInt(kv.getValue());
       break;
     }
     for(NullWritable element:counts)
       urlCount++;
     Put put =new Put(Bytes.toBytes(uidUrl.toString()));
     put.add(cf,count,Bytes.toBytes(urlCount));
     put.add(cf,ts,Bytes.toBytes(LoadUidUrlCount.timeRange));
     hTable.put(put);

     putNum++;
     if(putNum== HTableUtil.putBatch)
     {
       logger.info("putNum "+putNum);
       hTable.flushCommits();
     }
  }
}
