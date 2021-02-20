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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class MainTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new CliOverlord()},
        new Object[]{new CliBroker()},

        new Object[]{new FakeCliPeon(true)},
        new Object[]{new FakeCliPeon(false)},

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


  private static class FakeCliPeon extends CliPeon
  {
    List<String> forkTaskAndStatusFile = new ArrayList<String>();

    FakeCliPeon(boolean runningOnK8s)
    {
      forkTaskAndStatusFile.add("src/test/resources/task.json");
      forkTaskAndStatusFile.add("status.json");
      forkTaskAndStatusFile.add("report.json");

      try {
        Field privateField = CliPeon.class
                .getDeclaredField("taskAndStatusFile");
        privateField.setAccessible(true);
        privateField.set(this, forkTaskAndStatusFile);

        if (runningOnK8s) {
          HashMap<String, Object> k8sConfig = new HashMap<>();
          k8sConfig.put(CliPeon.IS_RUNNING_ON_K8S, true);
          Field privateMapField = CliPeon.class
                  .getDeclaredField("k8sConfig");
          privateMapField.setAccessible(true);
          privateMapField.set(this, k8sConfig);
        }
      }
      catch (Exception ex) {
        // do nothing.
      }

    }
  }
}
