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

import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.guice.GuiceInjectors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CliPeonTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCliPeonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "k8s");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonNonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "httpRemote");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonK8sANdWorkerIsK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), "k8sAndWorker");
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  private static class FakeCliPeon extends CliPeon
  {
    List<String> taskAndStatusFile = new ArrayList<>();

    FakeCliPeon(String taskDirectory, String runnerType)
    {
      try {
        taskAndStatusFile.add(taskDirectory);
        taskAndStatusFile.add("1");

        Field privateField = CliPeon.class
            .getDeclaredField("taskAndStatusFile");
        privateField.setAccessible(true);
        privateField.set(this, taskAndStatusFile);

        System.setProperty("druid.indexer.runner.type", runnerType);
        System.setProperty("druid.serverview.type", "http");
      }
      catch (Exception ex) {
        // do nothing
      }

    }
  }
}
