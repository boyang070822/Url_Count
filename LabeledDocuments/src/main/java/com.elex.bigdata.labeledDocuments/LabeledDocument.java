package com.elex.bigdata.labeledDocuments;

import com.elex.bigdata.util.MetricMapping;
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
 * Date: 3/24/14
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocument {
  public static void main(String[] args) throws IOException {
    ExecutorService service = new ThreadPoolExecutor(3, 8, 3600, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(20));
    /*
      args[0] inputPath args[1] outputPath

    */
    LDocCmdOption option = new LDocCmdOption();
    CmdLineParser parser = new CmdLineParser(option);
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
    for (String project : projects) {
      LDocProducer lDocProducer = new LDocProducer(option.inputBase, option.outputBase, option.localOutputBase, project, option.inputTime, option.outputTime,!option.notUseProject);
      service.execute(lDocProducer);
    }
    try {
      service.shutdown();
      service.awaitTermination(10, TimeUnit.MINUTES);
      System.out.println("get labeled docs completely");
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
