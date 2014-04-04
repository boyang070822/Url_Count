package com.elex.bigdata.labeledDocuments;

import org.junit.Test;

import java.io.*;
import java.util.*;

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

  @Test
  public void constructUrlCatgegories() throws IOException {
    Map<String,Set<String>> categoryUrls=new HashMap<String, Set<String>>();
    InputStream inputStream=this.getClass().getResourceAsStream("/url_category");
    BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
    String line=null;
    Map<String,String> categoriesMap=new HashMap<String, String>();
    categoriesMap.put("jogos","jogos");
    categoriesMap.put("compras","compras");
    while((line=reader.readLine())!=null){
        String[] tokens=line.split(" ");
        String category=categoriesMap.get(tokens[0]);
        if(category==null)category="other";
        for(int i=1;i<tokens.length;i++){
          Set<String> urls=categoryUrls.get(category);
          if(urls==null){
            urls=new HashSet<String>();
            categoryUrls.put(category,urls);
          }
          urls.add(tokens[i]);
        }
    }
    File dir=new File("/home/yb/windows/share/urls");
    for(File file :dir.listFiles())
    {
      BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
      String newLine=null;
      System.out.println(file.getCanonicalPath());
      while((newLine=bufferedReader.readLine())!=null){
        if(newLine.startsWith("http://"))
          newLine=newLine.substring(7);
        if(newLine.endsWith("/"))
          newLine=newLine.substring(0,newLine.length()-1);
        newLine.trim();
        Set<String> urls;
        if(file.getName().contains("ec")){
        urls=categoryUrls.get("jogos");
        if(urls==null){
          urls=new HashSet<String>();
          categoryUrls.put("jogos",urls);
        }
        }else if(file.getName().contains("game")){
          urls=categoryUrls.get("compras");
          if(urls==null){
            urls=new HashSet<String>();
            categoryUrls.put("compras",urls);
          }
        }else{
          urls=categoryUrls.get("other");
          if(urls==null){
            urls=new HashSet<String>();
            categoryUrls.put("other",urls);
          }
        }
        urls.add(newLine);
      }
    }
    BufferedWriter writer=new BufferedWriter(new FileWriter("/data/log/user_category/llda/url_category"));
    for(Map.Entry<String,Set<String>> entry: categoryUrls.entrySet()){
      StringBuilder builder=new StringBuilder();
      builder.append(entry.getKey());
      builder.append(" ");
      for(String url:entry.getValue()){
        builder.append(url+" ");
      }
      writer.write(builder.toString());
      writer.write("\n\r");
    }
  }

}
