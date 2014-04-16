package com.elex.bigdata.countuidurl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/4/14
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestOthers {
  @Test
  public void testBytes() throws ParseException {
    byte[] startRk= Bytes.add(new byte[]{1}, Bytes.toBytes("br"), Bytes.toBytes("20140403000000"));
    byte[] endRk=Bytes.add(new byte[]{1},Bytes.toBytes("br"),Bytes.toBytes("20140404000000"));
    System.out.println(Bytes.toStringBinary(startRk));
    System.out.println(Bytes.toStringBinary(endRk));
    DateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    long startTimeStamp=format.parse("20140414100101").getTime();
    System.out.println(startTimeStamp);
    System.out.println(Bytes.toStringBinary(Bytes.toBytes(startTimeStamp)));
  }

  @Test
  public void testThread() throws InterruptedException {
    ExecutorService service=new ThreadPoolExecutor(3,8,3600, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(20));
    for(int i=0;i<10;i++){
      service.execute(new Runnable() {
        @Override
        public void run() {
          for(int j=0;j<100;j++)
          {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            System.out.println("thread "+Thread.currentThread().getName());
          }

        }
      });
    }
    service.shutdown();
    service.awaitTermination(10,TimeUnit.MINUTES);
  }
  @Test
  public void testPath(){
     Path path=new Path("/user/hadoop/22find");
     System.out.println(path.toString());
     System.out.println(path.getName());
  }

}
