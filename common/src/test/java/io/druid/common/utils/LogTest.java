/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.common.utils;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

// The issue here is that parameters to the logging system are evaluated eagerly
// So CPU or resource heavy clauses in the log parameters get evaluated even if there is no debug logging
public class LogTest
{
  private static final Logger LOG = new Logger(LogTest.class);
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGoodLog()
  {
    final ExpensiveClass expensiveClass = new ExpensiveClass();
    if (LOG.isDebugEnabled()) {
      expectedException.expect(ISE.class);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Some error %s", expensiveClass.expensiveMethod());
    }
  }

  @Test
  @Ignore
  public void testBadLog()
  {
    final ExpensiveClass expensiveClass = new ExpensiveClass();
    if (LOG.isDebugEnabled()) {
      expectedException.expect(ISE.class);
    }
    LOG.debug("Some error %s", expensiveClass.expensiveMethod());
  }
}

class ExpensiveClass
{
  String expensiveMethod()
  {
    throw new ISE("Hogged up too much CPU time");
  }
}
