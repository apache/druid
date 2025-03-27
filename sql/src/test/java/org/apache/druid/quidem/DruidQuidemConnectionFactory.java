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

import com.google.inject.Injector;
import net.hydromatic.quidem.Quidem.ConnectionFactory;
import net.hydromatic.quidem.Quidem.PropertyHandler;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DruidQuidemConnectionFactory implements ConnectionFactory, PropertyHandler
{
  private Properties props = new Properties();
  private Map<String, String> envs = new HashMap<>();

  public DruidQuidemConnectionFactory()
  {
    // ensure driver loaded
    new DruidAvaticaTestDriver();
  }

  @Override
  public Connection connect(String name, boolean reference) throws Exception
  {
    URI uri = URI.create(name);
    if (uri.getScheme().endsWith("test")) {
      // This property will be set by DruidQuidemTestBase to select the supplier
      // for MultiComponentSupplier backed tests.
      // note: caching will be based on the effective config: so if the same
      // supplier is used from different MultiComponentSupplier it will be
      // shared.
      String customSupplier = props.getProperty("componentSupplier");
      if (customSupplier != null) {
        URIBuilder builder = new URIBuilder(uri);
        builder.addParameter("componentSupplier", customSupplier);
        builder.setHost("");
        name = builder.build().toString();
      }
      Connection connection = DriverManager.getConnection(name, props);
      envs = buildEnvsForConnection(connection);
      return connection;
    }
    throw new RuntimeException("unknown connection '" + name + "'");
  }

  private static Map<String, String> buildEnvsForConnection(Connection connection)
  {
    Map<String, String> envs = new HashMap<>();
    DruidConnectionExtras extras = DruidConnectionExtras.unwrapOrThrow(connection);
    Injector injector = extras.getInjector();
    QueryComponentSupplier supplier = injector.getInstance(QueryComponentSupplier.class);
    Class<? extends SqlEngine> engineClazz = supplier.getSqlEngineClass();
    String engineName = engineClazz.getName();
    boolean isDart = engineName.contains("Dart");
    boolean isMSQ = engineName.contains("MSQ");
    boolean isNative = engineName.contains("Native");

    envs.put("isDart", String.valueOf(isDart));
    envs.put("isMSQ", String.valueOf(isMSQ));
    envs.put("isNative", String.valueOf(isNative));
    envs.put("isTaskBased", String.valueOf(isDart || isMSQ));

    return envs;
  }

  public Object envLookup(String key)
  {
    return envs.get(key);
  }

  @Override
  public void onSet(String key, Object value)
  {
    props.setProperty(key, value.toString());
  }
}
