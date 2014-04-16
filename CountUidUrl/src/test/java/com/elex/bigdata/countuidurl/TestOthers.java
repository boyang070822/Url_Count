package com.elex.bigdata.countuidurl;

import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

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
  public void testBytes(){
    byte[] startRk= Bytes.add(new byte[]{1}, Bytes.toBytes("br"), Bytes.toBytes("20140403000000"));
    byte[] endRk=Bytes.add(new byte[]{1},Bytes.toBytes("br"),Bytes.toBytes("20140404000000"));
    System.out.println(Bytes.toStringBinary(startRk));
    System.out.println(Bytes.toStringBinary(endRk));
    System.out.println(Long.parseLong("20101010101010"));
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
