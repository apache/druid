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

package org.apache.druid.testsEx.config;

import org.apache.druid.java.util.common.logger.Logger;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class BaseJUnitRule
{
  private static final Logger LOG = new Logger(BaseJUnitRule.class);
  @Rule
  public TestWatcher watchman = new TestWatcher()
  {
    @Override
    public void starting(Description d)
    {
      LOG.info("RUNNING %s", d.getDisplayName());
    }

    @Override
    public void failed(Throwable e, Description d)
    {
      LOG.error("FAILED %s", d.getDisplayName());
    }

    @Override
    public void finished(Description d)
    {
      LOG.info("FINISHED %s", d.getDisplayName());
    }
  };
}
