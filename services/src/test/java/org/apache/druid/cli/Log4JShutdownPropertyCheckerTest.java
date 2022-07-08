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

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class Log4JShutdownPropertyCheckerTest
{
  @Test
  public void test_sets_the_stuff()
  {
    Log4JShutdownPropertyChecker checker = new Log4JShutdownPropertyChecker();
    Properties properties = new Properties();
    checker.checkProperties(properties);

    Assert.assertEquals(
        "org.apache.druid.common.config.Log4jShutdown",
        properties.get("log4j.shutdownCallbackRegistry")
    );
    Assert.assertEquals("true", properties.get("log4j.shutdownHookEnabled"));
    Assert.assertEquals("false", properties.get("log4j2.is.webapp"));
  }
}
