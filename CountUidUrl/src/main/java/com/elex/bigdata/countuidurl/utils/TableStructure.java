package com.elex.bigdata.countuidurl.utils;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/27/14
 * Time: 10:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class TableStructure {
   public static final String[] families={"basis","extend"};
   public static final String url="url",ip="ip",title="title",category="c";
   public static final String content="content";
   public static final String tableName="nav_all";
   public static final String adTableName="ad_all_log";
   public static final int projectIdIndex=0;
   public static final int nationIndex=1;
   public static final int timeIndex=3;
   public static final int uidIndex=17;

   public static final String url_Count_familiy="result";
   public static final String url_count_column_count="count";
   public static final String url_count_column_ts="timestamp";
}
