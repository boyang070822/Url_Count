package com.elex.bigdata.labeledDocuments;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/3/14
 * Time: 9:53 AM
 * To change this template use File | Settings | File Templates.
 */
import org.kohsuke.args4j.*;


public class LDocCmdOption {
  //args
  @Option(name="-project",usage = "specify the Project")
  public String project="";
  @Option(name="-nation",usage = "specify the nation")
  public String nation="";
  @Option(name="-inputBase",usage = "specify the UidUrlCount Text inputBase dir",required = true)
  public String inputBase="";
  @Option(name="-outputBase",usage = "specify the labeled Docs outputBase dir",required = true)
  public String outputBase="";
  @Option(name="-inputTime",usage = "specify the timeRange of UidUrlCount,usually timeHead",required = true)
  public String inputTime="";
  @Option(name="-outputTime",usage = "specify the outputTime of labeledDocs",required = true)
  public String outputTime="";
  @Option(name="-localOutputBase",usage ="specify the local output Base")
  public String localOutputBase="/data/user_category/llda/docs";
  @Option(name="-notUseProject",usage = "specify whether to use project to specify the inputDir ")
  public boolean notUseProject=false;
}
