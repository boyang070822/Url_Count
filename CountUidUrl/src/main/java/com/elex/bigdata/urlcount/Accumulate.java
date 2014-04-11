package com.elex.bigdata.urlcount;

import com.elex.bigdata.countuidurl.utils.CUUCmdOption;
import com.elex.bigdata.countuidurl.utils.TableStructure;
import com.elex.bigdata.util.MetricMapping;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.model.KeyRangeComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class Accumulate {
  String outputBase;
  String startTime;
  String endTime;
  private CUUCmdOption option;
  private Map<Byte,String> projectMap=new HashMap<Byte, String>();
  private ExecutorService service=new ThreadPoolExecutor(3,8,10, TimeUnit.MINUTES,new ArrayBlockingQueue<Runnable>(20));
  private FileSystem fs;
  private Configuration conf;
  public Accumulate(CUUCmdOption option) throws IOException {
    outputBase = option.outputBase;
    startTime = option.startTime;
    endTime = option.endTime;
    this.option = option;
    conf=HBaseConfiguration.create();
    fs=FileSystem.get(conf);
  }

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
    long t1 = System.currentTimeMillis();
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

    Accumulate accumulate = new Accumulate(option);
    accumulate.getUidUrl();

  }


  private List<KeyRange> getKeyRanges(String project, Set<String> nations) {
    List<KeyRange> keyRangeList = new ArrayList<KeyRange>();
    for (String nation : nations) {
      byte[] startRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(startTime));
      byte[] endRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(endTime));
      KeyRange keyRange = new KeyRange(startRk, true, endRk, false);
      keyRangeList.add(keyRange);
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRangeList, comparator);
    return keyRangeList;
  }

  private List<KeyRange> getSortedKeyRanges() {
    List<KeyRange> keyRanges = new ArrayList<KeyRange>();
    List<String> projects = new ArrayList<String>();
    if (!option.project.equals("")) {
      projects.add(option.project);
    } else {
      //todo
      //list all projects and add to list projects
      for (String project : MetricMapping.getInstance().getAllProjectShortNameMapping().keySet())
        projects.add(project);
    }
    for (String proj : projects) {
      Byte projectId = MetricMapping.getInstance().getProjectURLByte(proj);
      projectMap.put(projectId,proj);
      Set<String> nations = new HashSet<String>();
      System.out.println("projectId " + projectId + " project: " + proj);
      if (!option.nations.equals("")) {
        nations.add(option.nations);
      } else {
        //todo
        //get nations according to proj and execute the runner.
        long t3 = System.currentTimeMillis();
        Set<String> nationSet = MetricMapping.getNationsByProjectID(projectId);
        for (String nation : nationSet) {
          nations.add(nation);
        }
        System.out.println("get nations use " + (System.currentTimeMillis() - t3) + " ms");
      }
      if (nations.size() != 0 && projectId != null) {
        keyRanges.addAll(getKeyRanges(proj, nations));
      }
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRanges, comparator);
    return keyRanges;
  }

  private Scan getScan(){
    List<KeyRange> keyRanges=getSortedKeyRanges();
    Scan scan=new Scan();
    Filter filter=new SkipScanFilter(keyRanges);
    scan.setFilter(filter);
    scan.setStartRow(keyRanges.get(0).getLowerRange());
    scan.setStopRow(keyRanges.get(keyRanges.size()-1).getUpperRange());
    int cacheSize=5096;
    scan.setCaching(cacheSize);
    scan.addColumn(Bytes.toBytes(TableStructure.families[0]),Bytes.toBytes(TableStructure.url));
    return scan;
  }

  public void getUidUrl() throws IOException, InterruptedException {
    HTable hTable=new HTable(conf, TableStructure.tableName);
    ResultScanner scanner=hTable.getScanner(getScan());
    Map<String,Map<String,Map<String,Integer>>> projectUrlCountMap=new HashMap<String, Map<String, Map<String, Integer>>>();
    byte[] family=Bytes.toBytes(TableStructure.families[0]),urlQualify=Bytes.toBytes(TableStructure.url);
    for(Result result: scanner){
      byte[] rk=result.getRow();
      String project=projectMap.get(rk[0]);
      String uid=Bytes.toString(Arrays.copyOfRange(rk,TableStructure.uidIndex,rk.length));
      String url=Bytes.toString(result.getValue(family, urlQualify));
      Map<String,Map<String,Integer>> uidUrlCountMap=projectUrlCountMap.get(project);
      if(uidUrlCountMap==null){
        System.out.println("new project "+project);
        putToHdfs(projectUrlCountMap);
        projectUrlCountMap=new HashMap<String, Map<String, Map<String, Integer>>>();
        uidUrlCountMap=new HashMap<String, Map<String, Integer>>();
        projectUrlCountMap.put(project,uidUrlCountMap);
      }
      Map<String,Integer> urlCountMap=uidUrlCountMap.get(uid);
      if(urlCountMap==null){
        urlCountMap=new HashMap<String, Integer>();
        uidUrlCountMap.put(uid,urlCountMap);
      }
      Integer count=urlCountMap.get(url);
      if(count==null)
        urlCountMap.put(url,new Integer(1));
      else
        urlCountMap.put(url,count+1);
    }
    service.shutdown();
    service.awaitTermination(10,TimeUnit.MINUTES);
  }

  private void putToHdfs(Map<String, Map<String, Map<String, Integer>>> projectUrlCountMap) throws IOException {
    for(Map.Entry<String,Map<String,Map<String,Integer>>> entry:projectUrlCountMap.entrySet()){
      Path filePath=getOutputPath(entry.getKey());
      service.execute(new PutUrlCountRunnable(fs,filePath,entry.getValue()));
    }

  }

  private Path getOutputPath(String project) throws IOException {

    Path parentDir=new Path(outputBase+ File.separator+project+File.separator+startTime+"_"+endTime);
    if(!fs.exists(parentDir)){
      fs.mkdirs(parentDir);
    }
    Path outputFile=new Path(parentDir,"part-00000");
    return outputFile;
  }


}
