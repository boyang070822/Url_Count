package com.elex.bigdata.queryuidurl;


import com.elex.bigdata.queryuidurl.utils.HTableUtil;
import com.elex.bigdata.queryuidurl.utils.ScanRangeUtil;
import com.elex.bigdata.queryuidurl.utils.TableStructure;
import com.elex.bigdata.user_category.service.Submit;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/4/14
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryUidUrl {
  /*
    get uids in the last one hour(according to the CountHistoryUnit)
      set the scan startTime to now-countHistoryUnit
      set the scan endTime to now
      scan and get the uid.(Put to uid Set)
    for each uid
      set the start and end rk to scan resultTable(according to config)
      get uid_url:count key_value list to produce Map<url,count> and put it to Map<uid,Map<url,count>>

  */
   private static Logger logger=Logger.getLogger(QueryUidUrl.class);

   private Submit submit=null;
   public static void main(String[] args) throws ConfigurationException, ParseException, IOException {
     Date scanStartTime=null,scanEndTime=null;
     SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");

     if(args.length<1){
       scanEndTime=new Date();
       scanStartTime= ScanRangeUtil.getStartScanTime(scanEndTime, ScanRangeUtil.getCountHistoryUnit());
     }else {
       if(args[0].charAt(0)=='s'){
         scanStartTime=format.parse(args[0].substring(1));
         scanEndTime= ScanRangeUtil.getEndScanTime(scanStartTime, ScanRangeUtil.getCountHistoryUnit());
       }else if(args[0].charAt(0)=='e'){
         scanEndTime=format.parse(args[0].substring(1));
         scanStartTime= ScanRangeUtil.getStartScanTime(scanEndTime, ScanRangeUtil.getCountHistoryUnit());
       }else{
         logger.info("args[0] should start with s or e . or you can not have any args.");
         return;
       }
     }
     QueryUidUrl queryUidUrl=new QueryUidUrl();
     queryUidUrl.query(format.format(scanStartTime), format.format(scanEndTime));
   }

   public QueryUidUrl(){
      submit=QueryService.getQuerySubmit();
   }

   public void query(String startTime,String endTime ) throws ConfigurationException, IOException {
      List<String> urlCountTableNames=getUrlCountTableNames();
      List<String> uidTableNames=getUidTableNames();
      Configuration conf=HBaseConfiguration.create();
      for(int i=0;i<urlCountTableNames.size();i++){
        String urlCountTableName=urlCountTableNames.get(i);
        HTable urlCountTable=new HTable(conf,urlCountTableName);
        String uidTableName=uidTableNames.get(i);
        HTable uidTable=new HTable(conf,uidTableName);
        Set<String> uids=getUids(uidTable,startTime,endTime);
        Map<String,Map<String,Integer>> users=new HashMap<String, Map<String, Integer>>();
        logger.info("get uid url Counts");
        for(String uid: uids){
            Map<String,Integer> urlCounts=getUrlCounts(urlCountTable,uid);
            users.put(uid,urlCounts);
        }
        logger.info("submit Batch");
        submit.submitBatch(users);
      }
   }

   private List<String> getUrlCountTableNames() throws ConfigurationException {
     XMLConfiguration configuration=new XMLConfiguration("config.xml");
     String tableStr=configuration.getString(HTableUtil.Query_Tables);
     String[] tables=tableStr.split(",");
     return Arrays.asList(tables);
   }
   private List<String> getUidTableNames() throws ConfigurationException {
     XMLConfiguration configuration=new XMLConfiguration("config.xml");
     String tableStr=configuration.getString(HTableUtil.Query_Uid_Tables);
     String[] tables=tableStr.split(",");
     return Arrays.asList(tables);
   }

   private Set<String> getUids(HTable table,String startTime,String endTime) throws IOException {
     Set<String> uids=new HashSet<String>();
     Scan scan=new Scan();
     scan.setStartRow(Bytes.toBytes(startTime));
     scan.setStopRow(Bytes.toBytes(endTime));
     scan.addColumn(Bytes.toBytes(TableStructure.families[0]),Bytes.toBytes(TableStructure.url));
     ResultScanner scanner=table.getScanner(scan);
     for(Result result: scanner){
       String uid=Bytes.toString(result.getRow()).substring(TableStructure.uidIndex);
       uids.add(uid);
     }
     return uids;
   }

   private Map<String,Integer> getUrlCounts(HTable table,String uid) throws IOException {
     byte[] startRk=Bytes.toBytes(uid);
     int uidLen=startRk.length;
     byte[] endRk= HTableUtil.getNextRk(startRk);
     Map<String,Integer> urlCounts=new HashMap<String, Integer>();
     Scan scan=new Scan();
     scan.addColumn(Bytes.toBytes(TableStructure.url_Count_familiy),
                    Bytes.toBytes(TableStructure.url_count_column_count));
     scan.setStartRow(startRk);
     scan.setStopRow(endRk);
     ResultScanner scanner=table.getScanner(scan);
     for(Result result:scanner){
       for(KeyValue kv: result.raw()){
         String url=Bytes.toString(Arrays.copyOfRange(kv.getRow(),uidLen,kv.getRowLength()));
         Integer count=Bytes.toInt(kv.getValue());
         urlCounts.put(url,count);
       }
     }
     return urlCounts;
   }


}
