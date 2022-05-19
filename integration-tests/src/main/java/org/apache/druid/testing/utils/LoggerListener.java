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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.util.Arrays;

public class LoggerListener extends TestListenerAdapter
{
  private static final Logger LOG = new Logger(LoggerListener.class);

  @Override
  public void onTestFailure(ITestResult tr)
  {
    LOG.error(tr.getThrowable(), "Failed %s", formatTestName(tr));
  }

  @Override
  public void onTestSkipped(ITestResult tr)
  {
    LOG.warn("Skipped %s", formatTestName(tr));
  }

  @Override
  public void onTestSuccess(ITestResult tr)
  {
    LOG.info("Passed %s", formatTestName(tr));
  }

  @Override
  public void onTestStart(ITestResult tr)
  {
    LOG.info("Starting %s", formatTestName(tr));
  }

  private static String formatTestName(ITestResult tr)
  {
    if (tr.getParameters().length == 0) {
      return StringUtils.format("[%s.%s]", tr.getTestClass().getName(), tr.getName());
    } else {
      return StringUtils.format(
          "[%s.%s] with parameters %s",
          tr.getTestClass().getName(),
          tr.getName(),
          Arrays.toString(tr.getParameters())
      );
    }
  }
}
