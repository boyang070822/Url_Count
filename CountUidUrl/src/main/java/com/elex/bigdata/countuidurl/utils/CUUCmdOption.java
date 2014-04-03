package com.elex.bigdata.countuidurl.utils;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/2/14
 * Time: 5:52 PM
 * To change this template use File | Settings | File Templates.
 */
import org.kohsuke.args4j.*;
public class CUUCmdOption {
  @Option(name="-startTime",usage = "specify the users after startTime",required = true)
  public String startTime;
  @Option(name="-endTime",usage= "specify the users before endTime",required = true)
  public String endTime;
  @Option(name="-project",usage= "specify the project ")
  public String project="";
  @Option(name="-nations",usage= "specify the nations")
  public String nations="";
  @Option(name="-outputBase",usage= "specifty the hdfs outputBase",required = true)
  public String outputBase="";

}
