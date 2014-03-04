package com.elex.bigdata.countUrl;

import com.elex.bigdata.countuidurl.TableStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/27/14
 * Time: 11:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestPutData {

  @Test
  public void testPutData() throws IOException {
    String tableName = TableStructure.tableName;
    DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    String nation = "br";
    String[] urls = {"www.baidu.com", "www.sina.com", "www.google.com", "www.hao123.com", "www.facebook.com",
      "www.360.com", "www.youku.com", "p.xingcloud.com", "lib.tsinghua.edu.cn", "www.taobao.com"};
    int batch = 1000;
    Random random = new Random(100000l);
    testAndCreateTable(tableName);
    Configuration conf = HBaseConfiguration.create();
    System.out.print("start to get table "+tableName);
    HTable table = new HTable(conf, tableName);
    System.out.println("got Table");
    table.setAutoFlush(false);
    table.setWriteBufferSize(1000);
    System.out.println("get HTable");
    for (int j = 0; j < urls.length; j++) {
      for (int i = 0; i < batch; i++) {
        int uid = random.nextInt(10000);
        String time = format.format(new Date());
        String row = time + nation + uid;
        byte[] rk = Bytes.toBytes(row);
        Put put = new Put(rk);
        put.add(Bytes.toBytes(TableStructure.families[0]), Bytes.toBytes(TableStructure.url), Bytes.toBytes(urls[j]));
        table.put(put);
      }
      System.out.print("flush begin..");
      table.flushCommits();
      System.out.println("flush end");
    }
  }

  public void testAndCreateTable(String tableName) throws IOException {
    Configuration conf = new Configuration();
    HBaseAdmin admin= new HBaseAdmin(conf);
    if(!admin.tableExists(Bytes.toBytes(tableName))){
      HTableDescriptor descriptor=new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(TableStructure.families[0]));
      descriptor.addFamily(new HColumnDescriptor(TableStructure.families[1]));
      admin.createTable(descriptor);
    }
  }
  @Test
  public void testGetAndPut() throws IOException {
    String tableName = TableStructure.tableName;
    DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    String nation = "br";
    String[] urls = {"www.baidu.com", "www.sina.com", "www.google.com", "www.hao123.com", "www.facebook.com",
      "www.360.com", "www.youku.com", "p.xingcloud.com", "lib.tsinghua.edu.cn", "www.taobao.com"};
    int batch = 1000;
    Random random = new Random(100000l);
    Configuration conf = HBaseConfiguration.create();
    System.out.print("start to get table "+tableName);
    HTable table = new HTable(conf, tableName);
    System.out.println("got Table");
    table.setAutoFlush(false);
    table.setWriteBufferSize(1000);
    table.setScannerCaching(1000);
    System.out.println("get HTable");
    Scan scan=new Scan();
    byte[] startRow=Bytes.toBytes("000");
    byte[] stopRow=Bytes.toBytes("zzzz");
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]),Bytes.toBytes(TableStructure.url));
    ResultScanner results=table.getScanner(scan);
    for(Result result: results){
       System.out.println(System.currentTimeMillis());
       for(KeyValue kv: result.raw()){
         int count=Bytes.toInt(kv.getValue());
         Put put=new Put(kv.getRow());
         put.add(Bytes.toBytes(TableStructure.families[0]),Bytes.toBytes(TableStructure.url),Bytes.toBytes(count+1000));
         table.put(put);
       }
    }
    table.flushCommits();
  }
}
