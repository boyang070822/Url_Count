package com.elex.bigdata.counthistoryurl;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/4/14
 * Time: 9:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class CountHistoryUrl {
    /*
      count uid-url-count per hour and add to hbase table 'url_history_count_22find'
      use map reduce
        map get uid_url count key value
        reduce get value in 'url_history_count_22find' and increase
    */

}
