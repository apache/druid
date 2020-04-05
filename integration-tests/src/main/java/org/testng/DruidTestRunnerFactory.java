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

package /*CHECKSTYLE.OFF: PackageName*/org.testng/*CHECKSTYLE.ON: PackageName*/;

import com.google.inject.Injector;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.testng.internal.IConfiguration;
import org.testng.internal.annotations.IAnnotationFinder;
import org.testng.xml.XmlTest;

import java.util.List;

/**
 * This class must be in package org.testng to access protected methods like TestNG.getDefault().getConfiguration()
 */
public class DruidTestRunnerFactory implements ITestRunnerFactory
{
  private static final Logger LOG = new Logger(DruidTestRunnerFactory.class);

  @Override
  public TestRunner newTestRunner(ISuite suite, XmlTest test, List<IInvokedMethodListener> listeners)
  {
    IConfiguration configuration = TestNG.getDefault().getConfiguration();
    String outputDirectory = suite.getOutputDirectory();
    IAnnotationFinder annotationFinder = configuration.getAnnotationFinder();
    Boolean skipFailedInvocationCounts = suite.getXmlSuite().skipFailedInvocationCounts();
    return new DruidTestRunner(
      configuration,
      suite,
      test,
      outputDirectory,
      annotationFinder,
      skipFailedInvocationCounts,
      listeners
    );
  }

  private static class DruidTestRunner extends TestRunner
  {

    protected DruidTestRunner(
        IConfiguration configuration,
        ISuite suite,
        XmlTest test,
        String outputDirectory,
        IAnnotationFinder finder,
        boolean skipFailedInvocationCounts,
        List<IInvokedMethodListener> invokedMethodListeners
    )
    {
      super(configuration, suite, test, outputDirectory, finder, skipFailedInvocationCounts, invokedMethodListeners);
    }

    @Override
    public void run()
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
        runTests();
      }
      catch (Exception e) {
        LOG.error(e, "");
        throw new RuntimeException(e);
      }
      finally {
        lifecycle.stop();
      }

    }

    private void runTests()
    {
      super.run();
    }
  }
}
