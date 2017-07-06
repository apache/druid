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

package io.druid.java.util.common.logger;

import org.junit.Test;

public class LoggerTest
{
  @SuppressWarnings("MalformedFormatString")
  @Test
  public void testLogWithCrazyMessages()
  {
    final String message = "this % might %d kill %*.s the %s parser";
    final Logger log = new Logger(LoggerTest.class);
    log.warn(message);
  }

  @SuppressWarnings("MalformedFormatString")
  @Test
  public void testLegacyLogging()
  {
    final Logger log = new Logger(LoggerTest.class);
    final Throwable throwable = new Throwable();
    // These should show up in an IDE as deprecated, but shouldn't actually fail.
    log.error("foo", throwable);
    log.warn("foo", throwable);
  }
}
