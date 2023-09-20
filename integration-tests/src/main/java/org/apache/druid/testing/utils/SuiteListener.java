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

package org.apache.druid.testing.utils;

import com.google.inject.Injector;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.testng.ISuite;
import org.testng.ISuiteListener;

public class SuiteListener implements ISuiteListener
{
  private static final Logger LOG = new Logger(SuiteListener.class);

  @Override
  public void onStart(ISuite suite)
  {
    Injector injector = DruidTestModuleFactory.getInjector();
    IntegrationTestingConfig config = injector.getInstance(IntegrationTestingConfig.class);
    DruidClusterAdminClient druidClusterAdminClient = injector.getInstance(DruidClusterAdminClient.class);

    druidClusterAdminClient.waitUntilCoordinatorReady();
    druidClusterAdminClient.waitUntilIndexerReady();
    druidClusterAdminClient.waitUntilBrokerReady();
    String routerHost = config.getRouterUrl();
    if (null != routerHost) {
      druidClusterAdminClient.waitUntilRouterReady();
    }
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    try {
      lifecycle.start();
    }
    catch (Exception e) {
      LOG.error(e, "");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onFinish(ISuite suite)
  {
    Injector injector = DruidTestModuleFactory.getInjector();
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.stop();
  }
}
