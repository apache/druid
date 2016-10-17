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

package io.druid.testing.utils;

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import io.druid.java.util.common.logger.Logger;

public class LoggerListener extends TestListenerAdapter
{
  private static final Logger LOG = new Logger(LoggerListener.class);

  @Override
  public void onTestFailure(ITestResult tr)
  {
      LOG.info ("[%s] -- Test method failed", tr.getName());
  }

  @Override
  public void onTestSkipped(ITestResult tr)
  {
      LOG.info ("[%s] -- Test method skipped", tr.getName());
  }

  @Override
  public void onTestSuccess(ITestResult tr)
  {
      LOG.info ("[%s] -- Test method passed", tr.getName());
  }

  @Override
  public void onTestStart(ITestResult tr)
  {
      LOG.info ("[%s] -- TEST START", tr.getName() );
  }

}
