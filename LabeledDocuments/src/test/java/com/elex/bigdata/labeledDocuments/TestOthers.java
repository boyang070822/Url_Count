package com.elex.bigdata.labeledDocuments;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/24/14
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestOthers {
  @Test
  public void getResource() throws IOException {
    InputStream inputStream=this.getClass().getResourceAsStream("/url_category");
    BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
    String line=null;
    while((line=reader.readLine())!=null){
      System.out.println(line);
    }
    reader.close();
    inputStream.close();
  }

}
