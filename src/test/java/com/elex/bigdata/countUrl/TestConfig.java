package com.elex.bigdata.countUrl;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Test;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/3/14
 * Time: 10:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestConfig {
  @Test
  public void testReadConfig() throws ConfigurationException {
     String file="config.xml";
    Configuration conf= Config.createConfig(file, Config.ConfigFormat.xml);
    Configuration configuration=new XMLConfiguration(file);
    boolean contain=configuration.containsKey("ScanUnit.day");
    Object  unit=conf.getProperty("ScanUnit");
    Integer day=configuration.getInt("ScanUnit.day");
    Integer hour=configuration.getInt("ScanUnit.hour");
    System.out.println("hh");
  }
}
