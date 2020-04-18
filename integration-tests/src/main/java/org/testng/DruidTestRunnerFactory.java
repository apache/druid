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

import org.testng.internal.IConfiguration;
import org.testng.internal.annotations.IAnnotationFinder;
import org.testng.xml.XmlTest;

import java.util.List;

/**
 * This class must be in package org.testng to access protected methods like TestNG.getDefault().getConfiguration()
 */
public class DruidTestRunnerFactory implements ITestRunnerFactory
{
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
  }
}
