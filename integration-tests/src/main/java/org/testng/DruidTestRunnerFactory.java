/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.testng;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.client.util.Charsets;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
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
      waitUntilInstanceReady(client, config.getRouterHost());
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
                StatusResponseHolder response = client.get(
                    new URL(
                        String.format(
                            "http://%s/status",
                            host
                        )
                    )
                )
                                                      .go(handler)
                                                      .get();
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
          }, "Waiting for instance to be ready :" + host
      );
    }
  }
}
