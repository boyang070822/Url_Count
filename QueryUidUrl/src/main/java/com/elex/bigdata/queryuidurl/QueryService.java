package com.elex.bigdata.queryuidurl;

import com.caucho.hessian.client.HessianProxyFactory;
import com.elex.bigdata.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/6/14
 * Time: 11:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryService {
  private static final HessianProxyFactory WS_FACTORY = new HessianProxyFactory();
  private static Object SERVICE;
  static {

    URIBuilder builder = new URIBuilder();
    URI uri;
    builder.setScheme("http");
    Configuration conf= Config.createConfig("test/query_service.properties", Config.ConfigFormat.properties);
    String host=conf.getString("host");
    builder.setHost(host);
    int port=conf.getInt("port");
    builder.setPort(port);
    String path=conf.getString("path");
    builder.setPath(path);

    try {
      uri = builder.build();
      SERVICE = WS_FACTORY.create(uri.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static Submit getQuerySubmit(){
    return (Submit)SERVICE;
  }

}
