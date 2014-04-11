package com.elex.bigdata.labeledDocuments.getDocs;

import com.elex.bigdata.labeledDocuments.LDocCmdOption;
import com.elex.bigdata.labeledDocuments.LDocProducer;
import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/9/14
 * Time: 4:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetDocs {
  public static void main(String[] args) throws IOException {
    long t1=System.currentTimeMillis();
    LDocCmdOption option = new LDocCmdOption();
    CmdLineParser parser = new CmdLineParser(option);
    JobControl jobControl=new JobControl("lDocs");
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      parser.printUsage(System.out);
      return;
    }
    List<String> projects = new ArrayList<String>();
    if (option.project.equals("")) {
      //todo
      //get all projects
      for (String project : MetricMapping.getInstance().getAllProjectShortNameMapping().keySet()) {
        projects.add(project);
      }
    } else {
      projects.add(option.project);
    }
    List<LDocProducer.CopyLDocsToLocal> copyLDocsToLocalList=new ArrayList<LDocProducer.CopyLDocsToLocal>();
    for (String project : projects) {
      if(MetricMapping.getInstance().getProjectURLByte(project)!=null){
        CurrentDocProducer currentDocProducer = new CurrentDocProducer(option.inputBase, option.outputBase, option.localOutputBase, project,option.refInputTime, option.inputTime, option.outputTime,!option.notUseProject);
        copyLDocsToLocalList.add(new LDocProducer.CopyLDocsToLocal(option.outputBase,option.localOutputBase,project,option.outputTime));
        Job job=currentDocProducer.getJob();
        ControlledJob controlledJob=new ControlledJob(job.getConfiguration());
        controlledJob.setJob(job);
        jobControl.addJob(controlledJob);
      }
    }
    Thread jcThread = new Thread(jobControl);
    jcThread.start();
    ExecutorService service=new ThreadPoolExecutor(3,8,15,TimeUnit.MINUTES,new ArrayBlockingQueue<Runnable>(20));
    while(true){
      if(jobControl.allFinished()){
        System.out.println("all finished "+ "successful jobs "+jobControl.getSuccessfulJobList());
        jobControl.stop();
        for(LDocProducer.CopyLDocsToLocal copyLDocsToLocal :copyLDocsToLocalList){
          service.execute(copyLDocsToLocal);
        }
        service.shutdown();
        try {
          service.awaitTermination(10,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
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