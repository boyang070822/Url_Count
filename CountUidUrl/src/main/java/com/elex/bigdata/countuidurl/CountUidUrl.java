package com.elex.bigdata.countuidurl;

import com.elex.bigdata.countuidurl.utils.CUUCmdOption;
import com.elex.bigdata.countuidurl.utils.ScanRangeUtil;
import com.elex.bigdata.countuidurl.utils.TableStructure;
import com.elex.bigdata.util.MetricMapping;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.elex.bigdata.countuidurl.utils.ScanRangeUtil.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/26/14
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountUidUrl {
  private static Logger logger = Logger.getLogger(CountUidUrl.class);

  //the job runs once per day
  public static void main(String[] args) throws Exception {
    /*input has output Path(named with day(hour(minute)))
      if has the second arg,then it is the startTime.the Time should be format of 'yyyyMMddHHmmss';
      if not then set the endTime to currentTime. and the start time should be set to scanUnit before it.
    */
    /*
      first get the length of args.
      if the length <1 or >2 then return;
      if the length =1 then set the endTime and get ScanStartTime
      else if the length=2
              get the first Char of args[1],
              if it is 's', parse to the ScanStartTime and get ScanEndTime
              else if it is 'e',parse to the ScanEndTime and getScanStartTime
    */
    ExecutorService service=new ThreadPoolExecutor(8,30,3600,TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(200));
    CUUCmdOption option = new CUUCmdOption();
    CmdLineParser parser = new CmdLineParser(option);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace();
      System.out.println("CountUidUrl args.....");
      parser.printUsage(System.out);
      return;
    }

    String outputBase = option.outputBase;
    String startTime= option.startTime;
    String endTime = option.endTime;
    List<String> projects=new ArrayList<String>();
    if(!option.project.equals("")){
       projects.add(option.project);
    }else{
        //todo
        //list all projects and add to list projects
      for(String project : MetricMapping.getInstance().getAllProjectShortNameMapping().keySet())
        projects.add(project);
    }
    for(String proj: projects){
      Byte projectId=MetricMapping.getInstance().getProjectURLByte(proj);
      List<String> nations=new ArrayList<String>();
      if(!option.nations.equals("")){
         nations.add(option.nations);
      }else{
        //todo
        //get nations according to proj and execute the runner.
        Set<String> nationSet=MetricMapping.getNationsByProjectID(projectId);
        for(String nation: nations){
           nations.add(nation);
        }
      }
      if(nations.size()!=0)
        service.execute(new CountUidUrlRunner(proj,nations,startTime,endTime,outputBase));
    }
    service.shutdown();
    service.awaitTermination(3,TimeUnit.HOURS);
    System.out.println("service shutdown !");

  }


}
