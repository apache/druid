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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceInjectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MainTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new CliOverlord()},
        new Object[]{new CliBroker()},

        // Takes arguments. Cannot be used in this test
        //new Object[]{new CliPeon()},

        new Object[]{new CliHistorical()},
        new Object[]{new CliCoordinator()},

        // Implements Runnable, not GuiceRunnable
        //new Object[]{new CliHadoopIndexer()},

        // Takes arguments. Cannot be used in this test
        //new Object[]{new CliInternalHadoopIndexer()},

        new Object[]{new CliMiddleManager()},
        new Object[]{new CliRouter()},

        new Object[]{new CliIndexer()}
    );
  }

  private final GuiceRunnable runnable;

  public MainTest(GuiceRunnable runnable)
  {
    this.runnable = runnable;
  }

  @Test
  public void testSimpleInjection()
  {
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }
}
