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

import com.google.common.base.Stopwatch;
import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Launcher
{
  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore(
      x -> new ExposedAsBrokerQueryComponentSupplierWrapper(x)
  );
  private static final String QUIDEM_URI = "quidem.uri";
  private static Logger log = new Logger(Launcher.class);
  private final SqlTestFramework framework;
  private final ConfigurationInstance configurationInstance;
  private Lifecycle lifecycle;

  public Launcher(String uri) throws Exception
  {
    SqlTestFrameworkConfig config = SqlTestFrameworkConfig.fromURL(uri);
    configurationInstance = CONFIG_STORE.getConfigurationInstance(config);
    framework = configurationInstance.framework;
  }

  public void start() throws Exception
  {
    lifecycle = GuiceRunnable.initLifecycle(framework.injector(), log);
    if (withAutoStart()) {
      callQuidemStart();
    }
  }

  private void callQuidemStart() throws Exception
  {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet request = new HttpGet("http://localhost:12345/quidem/start");
    request.addHeader("Content-Type", "application/json");
    client.execute(request);
  }

  private boolean withAutoStart()
  {
    return Boolean.valueOf(System.getProperty("quidem.record.autostart", "false"));
  }

  public void shutdown()
  {
    lifecycle.stop();
  }

  public static void main(String[] args) throws Exception
  {
    String quidemUri = System.getProperty(QUIDEM_URI, "druidtest:///");
    Properties p = System.getProperties();
    for (Entry<Object, Object> entry : p.entrySet()) {
      Object key = entry.getKey();
      if (key.toString().startsWith("quidem")) {
        log.info("[%s] -> %s", key, entry.getValue());
      }
    }
    log.info("Starting Quidem with URI[%s]", quidemUri);
    Stopwatch stopwatch = Stopwatch.createStarted();
    Launcher launcher = new Launcher(quidemUri);
    log.info("Framework creation time: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    launcher.start();
    log.info("Total time to launch: %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    launcher.lifecycle.join();
  }
}
