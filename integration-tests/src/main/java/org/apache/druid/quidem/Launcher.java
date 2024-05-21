/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.quidem;

import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;

public class Launcher
{
  public static final String URI_PREFIX = "druidtest://";
  public static final String DEFAULT_URI = URI_PREFIX + "/";
  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore();

  private static Logger log = new Logger(Launcher.class);

  public static SqlTestFrameworkConfig buildConfigfromURIParams(String url) throws SQLException
  {
    Map<String, String> queryParams;
    queryParams = new HashMap<>();
    try {
      List<NameValuePair> params = URLEncodedUtils.parse(new URI(url), StandardCharsets.UTF_8);
      for (NameValuePair pair : params) {
        queryParams.put(pair.getName(), pair.getValue());
      }
      // possible caveat: duplicate entries overwrite earlier ones
    }
    catch (URISyntaxException e) {
      throw new SQLException("Can't decode URI", e);
    }

    return new SqlTestFrameworkConfig(queryParams);
  }

  @Test
  public void runIt() throws Exception
  {
    Launcher.main3(null);
  }

  private static ConfigurationInstance getCI2() throws SQLException, Exception
  {
    SqlTestFrameworkConfig config = buildConfigfromURIParams("druidtest:///");

    ConfigurationInstance ci = CONFIG_STORE.getConfigurationInstance(
        config,
        x -> new ExposedAsBrokerQueryComponentSupplierWrapper(x)
    );
    return ci;
  }

  private static void main3(Object object) throws Exception
  {

    SqlTestFramework framework = getCI2().framework;

    if (true) {
      Lifecycle lifecycle = GuiceRunnable.initLifecycle(framework.injector(), log);

      chk1();
      chkStatus();

      lifecycle.stop();
    } else {

    }

  }

  private static void chk1() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:12345/druid/v2/sql"))
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString("{\"query\":\"Select * from foo\"}"))
        .build();
    System.out.println(request);
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println(a);
    assertNotEquals(400, a.statusCode());
  }

  private static void chkStatus() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:12345/status"))
        .header("Content-Type", "application/json")
        .GET()
        .build();
    System.out.println(request);
    // request.
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println(a);
    assertNotEquals(400, a.statusCode());
  }

}
