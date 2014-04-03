package com.elex.bigdata.labeledDocuments;

import com.elex.bigdata.util.MetricMapping;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocument {
  public static void main(String[] args) throws IOException {
     ExecutorService service= new ThreadPoolExecutor(3,20,3600, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(20));
    /*
      args[0] inputPath args[1] outputPath

    */
    LDocCmdOption option=new LDocCmdOption();
    CmdLineParser parser=new CmdLineParser(option);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      parser.printUsage(System.out);
      return;
    }
    List<String> projects=new ArrayList<String>();
    if(option.project.equals("")){
      //todo
      //get all projects
      for(String project :MetricMapping.getInstance().getAllProjectShortNameMapping().keySet()){
        projects.add(project);
      }
    }else{
      projects.add(option.project);
    }
    for(String project:projects){
       if(option.nation.equals("")){
         //todo
         //get all nations and construct LDocProducer to execute
         Byte projectId=MetricMapping.getInstance().getProjectURLByte(project);
         Set<String> nations=MetricMapping.getNationsByProjectID(projectId);
         for(String nation:nations){
           LDocProducer lDocProducer=new LDocProducer(option.inputBase,option.outputBase,option.localOutputBase,project,nation,option.inputTime,option.outputTime);
           service.execute(lDocProducer);
         }
       }else{
          LDocProducer lDocProducer=new LDocProducer(option.inputBase,option.outputBase,option.localOutputBase,project,option.nation,option.inputTime,option.outputTime);
          service.execute(lDocProducer);
       }
    }
    try {
      service.shutdown();
      service.awaitTermination(10,TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
