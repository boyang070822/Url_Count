package com.elex.bigdata.countUrl;

import org.junit.Test;

import java.text.ParseException;

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
       System.out.println(CountUrl.getNextDay(day));
     }
  }
}
