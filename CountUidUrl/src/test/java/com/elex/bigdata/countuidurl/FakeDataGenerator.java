package com.elex.bigdata.countuidurl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/7/14
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class FakeDataGenerator {
  private String[] urls={"www.filmesdecinema.com.br","br.msn.com","br.yahoo.com","www.lancenet.com.br",
                         "clickjogos.uol.com.br","www.ojogos.com.br","www.papajogos.com.br","webmotors.com.br",
                         "www.mercadolivre.com.br","www.magazineluiza.com.br","www.americanas.com.br","www.sitedegames.com"};
  private int[] frequents={300,500,300,100,400,200,800,1000,1200,1100,1200,1300};
  private Random uidRand=new Random(1000000l);
  private SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
  @Test
  public void generateDataToNav() throws IOException {
    assert urls.length==frequents.length;
    Configuration conf= HBaseConfiguration.create();
    String tableName="nav_22find";
    byte[] family=Bytes.toBytes("basis"),column=Bytes.toBytes("url");
    HTable table=new HTable(conf,tableName);
    table.setAutoFlush(false);
    for(int i=0;i<urls.length;i++){
       String url=urls[i];
       for(int j=0;j<frequents[i];j++){
         String time=format.format(new Date());
         String uid=String.valueOf(uidRand.nextInt(100000));
         byte[] rk= Bytes.toBytes(time+"br"+uid);
         Put put=new Put(rk);
         put.add(family,column,Bytes.toBytes(url));
         table.put(put);
       }
       table.flushCommits();
    }
  }
}
