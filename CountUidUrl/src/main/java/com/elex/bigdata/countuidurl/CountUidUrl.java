package com.elex.bigdata.countuidurl;

import com.elex.bigdata.countuidurl.utils.CUUCmdOption;
import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


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
    long t1=System.currentTimeMillis();
    CUUCmdOption option = new CUUCmdOption();
    CmdLineParser parser = new CmdLineParser(option);
    JobControl jobControl=new JobControl("CountUidUrl");
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
      System.out.println("projectId "+projectId+" project: "+proj);
      if(!option.nations.equals("")){
         nations.add(option.nations);
      }else{
        //todo
        //get nations according to proj and execute the runner.
        Set<String> nationSet=MetricMapping.getNationsByProjectID(projectId);
        for(String nation: nationSet){
           nations.add(nation);
        }
      }
      if(nations.size()!=0&&projectId!=null)
      {
        Job job=new CountUidUrlRunner(proj,nations,startTime,endTime,outputBase).getJob();
        ControlledJob controlledJob=new ControlledJob(job.getConfiguration());
        controlledJob.setJob(job);
        jobControl.addJob(controlledJob);
      }
    }
    Thread jcThread = new Thread(jobControl);
    jcThread.start();
    while(true){
      if(jobControl.allFinished()){
        System.out.println("all finished "+ "successful jobs "+jobControl.getSuccessfulJobList());
        jobControl.stop();
        System.out.println("count use "+(System.currentTimeMillis()-t1)+" ms");
        return ;
      }
      if(jobControl.getFailedJobList().size() > 0){
        System.out.println("failed jobs "+ jobControl.getFailedJobList());
        jobControl.stop();
        System.out.println("count use "+(System.currentTimeMillis()-t1)+" ms");
        return ;
      }
    }
  }


}
