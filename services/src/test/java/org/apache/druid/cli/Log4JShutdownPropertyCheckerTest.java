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

package org.apache.druid.cli;

import org.apache.druid.common.config.Log4jShutdown;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.ShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.junit.Assert;
import org.junit.Test;

public class Log4JShutdownPropertyCheckerTest
{
  @Test
  public void test_log4jShtutdown_isStarted()
  {
    Log4JShutdownPropertyChecker checker = new Log4JShutdownPropertyChecker();
    checker.checkProperties(System.getProperties());
    final LoggerContextFactory contextFactory = LogManager.getFactory();

    if (!(contextFactory instanceof Log4jContextFactory)) {
      Assert.fail();
    }
    final ShutdownCallbackRegistry registry = ((Log4jContextFactory) contextFactory).getShutdownCallbackRegistry();
    if (!(registry instanceof Log4jShutdown)) {
      Assert.fail();
    }

    Assert.assertTrue(((Log4jShutdown) registry).isStarted());
  }
}