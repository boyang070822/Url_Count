package com.elex.bigdata.countuidurl;

import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/4/14
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestOthers {
  @Test
  public void testBytes(){
    byte[] startRk= Bytes.add(new byte[]{1}, Bytes.toBytes("br"), Bytes.toBytes("20140403000000"));
    byte[] endRk=Bytes.add(new byte[]{1},Bytes.toBytes("br"),Bytes.toBytes("20140404000000"));
    System.out.println(Bytes.toStringBinary(startRk));
    System.out.println(Bytes.toStringBinary(endRk));

  }

}
