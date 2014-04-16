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
import org.apache.hadoop.hbase.KeyValue;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
  private Map<Byte, String> projectMap = new HashMap<Byte, String>();
  private ExecutorService service = new ThreadPoolExecutor(3, 8, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(20));
  private FileSystem fs;
  private Configuration conf;
  private static String jogosUrl="www.jogos.com",comprasUrl="www.compras.com",otherUrl="www.other.com";
  private long startTimeStamp,endTimeStamp;
  public Accumulate(CUUCmdOption option) throws IOException, ParseException {
    outputBase = option.outputBase;
    startTime = option.startTime;
    endTime = option.endTime;
    this.option = option;
    conf = HBaseConfiguration.create();
    fs = FileSystem.get(conf);
    DateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    startTimeStamp=format.parse(startTime).getTime();
    endTimeStamp=format.parse(endTime).getTime();
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
    accumulate.getAdUidUrl();
    accumulate.shutdown();
  }

  //if timestamp in rk is long type or string ,rk should be different
  private List<KeyRange> getKeyRanges(String project, Set<String> nations,boolean timeAsLong) {
    List<KeyRange> keyRangeList = new ArrayList<KeyRange>();
    for (String nation : nations) {
      byte[] startRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), timeAsLong?Bytes.toBytes(startTimeStamp):Bytes.toBytes(startTime));
      byte[] endRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), timeAsLong?Bytes.toBytes(endTimeStamp):Bytes.toBytes(endTime));
      KeyRange keyRange = new KeyRange(startRk, true, endRk, false);
      keyRangeList.add(keyRange);
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRangeList, comparator);
    return keyRangeList;
  }

  private List<KeyRange> getSortedKeyRanges(boolean timeAsLong) {
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
      projectMap.put(projectId, proj);
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
        keyRanges.addAll(getKeyRanges(proj, nations,timeAsLong));
      }
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRanges, comparator);
    for(KeyRange keyRange : keyRanges){
      System.out.println("add keyRange "+Bytes.toStringBinary(keyRange.getLowerRange())+"---"+Bytes.toStringBinary(keyRange.getUpperRange()));
    }
    return keyRanges;
  }


  private Scan getScan(Map<String, List<String>> familyColumns,boolean timeAsLong) {
    List<KeyRange> keyRanges = getSortedKeyRanges(timeAsLong);
    Scan scan = new Scan();
    Filter filter = new SkipScanFilter(keyRanges);
    scan.setFilter(filter);
    scan.setStartRow(keyRanges.get(0).getLowerRange());
    scan.setStopRow(keyRanges.get(keyRanges.size() - 1).getUpperRange());
    int cacheSize = 5096;
    scan.setCaching(cacheSize);
    for (Map.Entry<String, List<String>> entry : familyColumns.entrySet()) {
      String family = entry.getKey();
      for (String column : entry.getValue()) {
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
      }
    }

    return scan;
  }

  public void getUidUrl() throws IOException, InterruptedException {
    HTable hTable = new HTable(conf, TableStructure.tableName);
    Map<String, List<String>> familyColumns = new HashMap<String, List<String>>();
    List<String> columns = new ArrayList<String>();
    columns.add(TableStructure.url);
    familyColumns.put(TableStructure.families[0], columns);
    ResultScanner scanner = hTable.getScanner(getScan(familyColumns,false));
    Map<String, Map<String, Map<String, Integer>>> projectUrlCountMap = new HashMap<String, Map<String, Map<String, Integer>>>();
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        byte[] rk = kv.getRow();
        String project = projectMap.get(rk[0]);
        String uid = Bytes.toString(Arrays.copyOfRange(rk, TableStructure.uidIndex, rk.length));
        String url = Bytes.toString(kv.getValue());
        Map<String, Map<String, Integer>> uidUrlCountMap = projectUrlCountMap.get(project);
        if (uidUrlCountMap == null) {
          if (projectUrlCountMap.size() != 0)
            putToHdfs(projectUrlCountMap,"custom");
          projectUrlCountMap = new HashMap<String, Map<String, Map<String, Integer>>>();
          uidUrlCountMap = new HashMap<String, Map<String, Integer>>();
          projectUrlCountMap.put(project, uidUrlCountMap);
        }
        Map<String, Integer> urlCountMap = uidUrlCountMap.get(uid);
        if (urlCountMap == null) {
          urlCountMap = new HashMap<String, Integer>();
          uidUrlCountMap.put(uid, urlCountMap);
        }
        Integer count = urlCountMap.get(url);
        if (count == null)
          urlCountMap.put(url, new Integer(1));
        else
          urlCountMap.put(url, count + 1);
      }
    }
  }
  /*
     get uid category from ad_all_log ;
     transfer category to a url,and give it a count 3.
   */
  public void getAdUidUrl() throws IOException, InterruptedException {
    HTable hTable = new HTable(conf, TableStructure.adTableName);
    Map<String, List<String>> familyColumns = new HashMap<String, List<String>>();
    List<String> columns = new ArrayList<String>();
    columns.add(TableStructure.category);
    familyColumns.put(TableStructure.families[0], columns);
    ResultScanner scanner = hTable.getScanner(getScan(familyColumns,true));

    byte[] family = Bytes.toBytes(TableStructure.families[0]);
    byte[] categoryColumn = Bytes.toBytes(TableStructure.category);
    Map<Integer, String> categoryToUrlMap = new HashMap<Integer, String>();
    categoryToUrlMap.put(new Integer(0), otherUrl);
    categoryToUrlMap.put(new Integer(1), jogosUrl);
    categoryToUrlMap.put(new Integer(2), comprasUrl);
    categoryToUrlMap.put(new Integer(99), otherUrl);
    Map<String, Map<String, Map<String, Integer>>> projectUrlCountMap = new HashMap<String, Map<String, Map<String, Integer>>>();
    for (Result result : scanner) {
      byte[] rk = result.getRow();
      String project = projectMap.get(rk[0]);
      String uid = Bytes.toString(Arrays.copyOfRange(rk, TableStructure.adUidIndex, rk.length));
      int category = Integer.parseInt(Bytes.toString(result.getValue(family, categoryColumn)));
      String url = categoryToUrlMap.get(category);
      Map<String, Map<String, Integer>> uidUrlCountMap = projectUrlCountMap.get(project);
      if (uidUrlCountMap == null) {
        if (projectUrlCountMap.size() != 0)
          putToHdfs(projectUrlCountMap,"ad");
        projectUrlCountMap = new HashMap<String, Map<String, Map<String, Integer>>>();
        uidUrlCountMap = new HashMap<String, Map<String, Integer>>();
        projectUrlCountMap.put(project, uidUrlCountMap);
      }
      Map<String, Integer> urlCountMap = uidUrlCountMap.get(uid);
      if (urlCountMap == null) {
        urlCountMap = new HashMap<String, Integer>();
        uidUrlCountMap.put(uid, urlCountMap);
      }
      Integer count = urlCountMap.get(url);
      if (count == null)
        urlCountMap.put(url, new Integer(3));
      else
        urlCountMap.put(url, count + 3);
    }
  }

  private void putToHdfs(Map<String, Map<String, Map<String, Integer>>> projectUrlCountMap,String flag) throws IOException {
    for (Map.Entry<String, Map<String, Map<String, Integer>>> entry : projectUrlCountMap.entrySet()) {
      Path filePath = getOutputPath(entry.getKey(),flag);
      System.out.println("put to hdfs " + entry.getKey());
      service.execute(new PutUrlCountRunnable(fs, filePath, entry.getValue()));
    }

  }

  private Path getOutputPath(String project,String flag) throws IOException {

    Path parentDir = new Path(outputBase + File.separator + project + File.separator + startTime + "_" + endTime);
    if (!fs.exists(parentDir)) {
      fs.mkdirs(parentDir);
    }
    Path outputFile = new Path(parentDir, "part-"+flag);
    return outputFile;
  }

  public void shutdown() throws InterruptedException {
    service.shutdown();
    service.awaitTermination(10,TimeUnit.MINUTES);
  }


}
