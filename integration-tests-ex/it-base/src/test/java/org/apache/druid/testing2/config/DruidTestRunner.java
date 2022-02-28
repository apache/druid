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

package org.apache.druid.testing2.config;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * Test runner for Druid integration tests. Loads test configuration. Initializes
 * Guice. Injects dependencies into the test. Runs the tests.
 * Shuts down afterwards. Since cluster configuration and health checks are a bit
 * expensive, does that work once per test class rather than once per test method.
 * <p>
 * Note that our Guice usage is a bit clunky. Druid modules define objects that
 * must be lifecycle managed, but as lazy singletons, which means that they might
 * be created after the lifecycle starts, which causes the dreaded "it doesn't
 * work that way" message. The awkward workaround is to ask to inject test members
 * <i>before</i> starting the lifecycle, so that the injection creates a reference,
 * which creates the object, which registers it in the lifecycle. We should fix
 * this mess. Until then, the awkwardness is hidden in this test runner.
 */
public class DruidTestRunner extends BlockJUnit4ClassRunner
{
  public class CloseConfig extends Statement
  {
    private final Statement next;

    public CloseConfig(Statement next)
    {
      this.next = next;
    }

    @Override
    public void evaluate() throws Throwable
    {
      next.evaluate();
      if (initializer != null) {
        initializer.close();
        initializer = null;
      }
    }
  }

  private Initializer initializer;

  public DruidTestRunner(Class<?> testClass) throws InitializationError
  {
    super(testClass);
  }

  @Override
  protected Object createTest() throws Exception
  {
    Object test = super.createTest();
    if (initializer == null) {
      initializer = Initializer.quickBuild(test);
    } else {
      initializer.injector().injectMembers(test);
    }
    return test;
  }

  @Override
  protected Statement withAfterClasses(Statement statement)
  {
    return new CloseConfig(super.withAfterClasses(statement));
  }
}
