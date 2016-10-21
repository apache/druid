/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.testng;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.internal.IConfiguration;
import org.testng.internal.annotations.IAnnotationFinder;
import org.testng.xml.XmlTest;

import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;

public class DruidTestRunnerFactory implements ITestRunnerFactory
{
  private static final Logger LOG = new Logger(DruidTestRunnerFactory.class);

  @Override
  public TestRunner newTestRunner(
      ISuite suite, XmlTest test, List<IInvokedMethodListener> listeners
  )
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
      HttpClient client = injector.getInstance(Key.get(HttpClient.class, Global.class));
      ;
      waitUntilInstanceReady(client, config.getCoordinatorHost());
      waitUntilInstanceReady(client, config.getIndexerHost());
      waitUntilInstanceReady(client, config.getBrokerHost());
      String routerHost = config.getRouterHost();
      if (null != routerHost) {
	  waitUntilInstanceReady(client, config.getRouterHost());
      }
      Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
      try {
        lifecycle.start();
        runTests();
      }
      catch (Exception e) {
        e.printStackTrace();
        throw Throwables.propagate(e);
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
      final StatusResponseHandler handler = new StatusResponseHandler(Charsets.UTF_8);
      RetryUtil.retryUntilTrue(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              try {
                StatusResponseHolder response = client.go(
                    new Request(
                        HttpMethod.GET,
                        new URL(
                            String.format(
                                "http://%s/status",
                                host
                            )
                        )
                    ),
                    handler
                ).get();

                System.out.println(response.getStatus() + response.getContent());
                if (response.getStatus().equals(HttpResponseStatus.OK)) {
                  return true;
                } else {
                  return false;
                }
              }
              catch (Throwable e) {
                e.printStackTrace();
                return false;
              }
            }
          }, "Waiting for instance to be ready: [" + host + "]"
      );
    }
  }
}
