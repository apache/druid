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
import com.google.inject.Key;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.internal.IConfiguration;
import org.testng.internal.annotations.IAnnotationFinder;
import org.testng.xml.XmlTest;

import java.net.URL;
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
      HttpClient client = injector.getInstance(Key.get(HttpClient.class, TestClient.class));

      waitUntilInstanceReady(client, config.getCoordinatorUrl());
      waitUntilInstanceReady(client, config.getIndexerUrl());
      waitUntilInstanceReady(client, config.getBrokerUrl());
      String routerHost = config.getRouterUrl();
      if (null != routerHost) {
        waitUntilInstanceReady(client, config.getRouterUrl());
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

    public void waitUntilInstanceReady(final HttpClient client, final String host)
    {
      RetryUtil.retryUntilTrue(
          () -> {
            try {
              StatusResponseHolder response = client.go(
                  new Request(HttpMethod.GET, new URL(StringUtils.format("%s/status/health", host))),
                  StatusResponseHandler.getInstance()
              ).get();

              LOG.info("%s %s", response.getStatus(), response.getContent());
              return response.getStatus().equals(HttpResponseStatus.OK);
            }
            catch (Throwable e) {
              LOG.error(e, "");
              return false;
            }
          },
          "Waiting for instance to be ready: [" + host + "]"
      );
    }
  }
}
