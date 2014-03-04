package com.elex.bigdata.countUrl;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.elex.bigdata.utils.ScanRangeUtil.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/27/14
 * Time: 11:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestCountUrl {
  @Test
  public void testGetNextDay() throws ParseException {
     String[] days={"20140131","20140228","20140430"};
     for(String day : days){
       System.out.println(getNextDay(day));
     }
  }
  @Test
  public void testScanTime() throws ConfigurationException, ParseException {
    String day="s20140220000000";
    DateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    Date startScanTime = null,endScanTime = null;
    Date scanUnitTime=getScanUnit();
    char type=day.charAt(0);
    if(type=='s'){
      startScanTime=format.parse(day.substring(1));
      endScanTime=getEndScanTime(startScanTime,scanUnitTime);
    }
    else if(type=='e'){
      endScanTime=format.parse(day.substring(1));
      startScanTime=getStartScanTime(endScanTime,scanUnitTime);
    }
    System.out.println("hh");
  }
  @Test
  public void testTextMap(){
    Text url1=new Text("www.sina.com");
    Text url2=new Text("www.sina.com");
    Map<Text,Integer> map=new HashMap<Text,Integer>();
    map.put(url1,new Integer(0));
    Integer num=map.get(url1);
    map.put(url1,++num);
    Integer num2=map.get(url2);
    if(num2!=null)
      num2++;
    else
      num2++;
    System.out.print("hello");
    System.out.println(url1.equals(url2));
  }
}
