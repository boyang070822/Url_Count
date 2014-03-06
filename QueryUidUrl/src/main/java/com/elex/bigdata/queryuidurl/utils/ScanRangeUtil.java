package com.elex.bigdata.queryuidurl.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class ScanRangeUtil {
  public static final String SCAN_UNIT_DAY="ScanUnit.day";
  public static final String SCAN_UNIT_HOUR="ScanUnit.hour";
  public static final String SCAN_UNIT_MINUTE="ScanUnit.minute";
  public static final String COUNT_HISTORY_UNIT_HOUR="CountHistoryUnit.hour";
  public static final String COUNT_HISTORY_UNIT_MINUTE="CountHistoryUnit.minute";
  //knowing the end time and the scanUnit,get the scan startTime
  public static Date getStartScanTime(Date endTime,Date ScanUnit){
    Date startDate=new Date();
    startDate.setTime(endTime.getTime()-ScanUnit.getTime());
    return startDate;
  }

  //knowing the start time and the scanUnit, get the scan end time
  public static Date getEndScanTime(Date startTime,Date scanUnit){
    Date endDate=new Date();
    System.out.println(startTime.getTime()+" "+scanUnit.getTime());
    endDate.setTime(startTime.getTime()+scanUnit.getTime());
    return endDate;
  }

  //get the ScanUnit
  public static Date getScanUnit() throws ConfigurationException {
    XMLConfiguration configuration=new XMLConfiguration("config.xml");
    Date scanUnitTime=new Date();
    scanUnitTime.setTime(0l);
    int day=configuration.getInt(SCAN_UNIT_DAY),hour=configuration.getInt(SCAN_UNIT_HOUR),
      minute=configuration.getInt(SCAN_UNIT_MINUTE);
    scanUnitTime.setDate(scanUnitTime.getDate()+day);
    scanUnitTime.setHours(scanUnitTime.getHours()+hour);
    scanUnitTime.setMinutes(scanUnitTime.getMinutes()+minute);
    return scanUnitTime;
  }

  public static Date getCountHistoryUnit() throws ConfigurationException {
    XMLConfiguration configuration=new XMLConfiguration("config.xml");
    Date scanUnitTime=new Date();
    scanUnitTime.setTime(0l);
    int hour=configuration.getInt(COUNT_HISTORY_UNIT_HOUR),
      minute=configuration.getInt(COUNT_HISTORY_UNIT_MINUTE);
    scanUnitTime.setHours(scanUnitTime.getHours()+hour);
    scanUnitTime.setMinutes(scanUnitTime.getMinutes()+minute);
    return scanUnitTime;
  }

  public static String getNextDay(String day) throws ParseException {
    DateFormat format=new SimpleDateFormat("yyyyMMdd");
    Date date=format.parse(day);
    date.setDate(date.getDate()+1);
    return format.format(date);
  }
}
