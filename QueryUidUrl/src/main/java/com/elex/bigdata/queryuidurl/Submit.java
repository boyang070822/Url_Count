package com.elex.bigdata.queryuidurl;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/6/14
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Submit {
  public void submit(String uid,Map<String,Integer> user);
  public void submitBatch(Map<String,Map<String,Integer>> users);
}
