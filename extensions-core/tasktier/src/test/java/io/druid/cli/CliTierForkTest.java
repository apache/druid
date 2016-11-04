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

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.RemoteChatHandler;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.BusyTask;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.overlord.resources.DeadhandResource;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeoutException;

public class CliTierForkTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testGetModules() throws Exception
  {
    final File lockFile = temporaryFolder.newFile();
    Assert.assertTrue(lockFile.delete() || !lockFile.exists());
    Assert.assertFalse(lockFile.exists());
    final Task task = new BusyTask("taskId", lockFile.toString(), 100);

    final CliTierFork cliTierFork = new CliTierFork();
    cliTierFork.taskAndStatusFile = ImmutableList.of(
        temporaryFolder.newFile().toString(), // Should not actually read from this
        temporaryFolder.newFile().toString()
    );
    final Injector startupInjector = GuiceInjectors.makeStartupInjector();
    startupInjector.injectMembers(cliTierFork);

    final Injector injector = Initialization.makeInjectorWithModules(
        startupInjector,
        ImmutableList.of(
            Modules.override(cliTierFork.getModules()).with(
                new Module()
                {
                  @Override
                  public void configure(Binder binder)
                  {
                    JsonConfigProvider.bindInstance(
                        binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
                    );
                    JsonConfigProvider.bindInstance(
                        binder,
                        Key.get(DruidNode.class, RemoteChatHandler.class),
                        new DruidNode("test", "localhost", null)
                    );
                    JsonConfigProvider.bindInstance(
                        binder,
                        Key.get(ServerConfig.class, RemoteChatHandler.class),
                        new ServerConfig()
                    );
                    binder.bind(Task.class).toInstance(task);
                  }
                })
        )
    );
    final Binding<ForkAnnouncer> binding = injector.getBinding(ForkAnnouncer.class);
    Assert.assertNotNull(binding);
  }

  @Test
  public void testParentMonitorInputStreamFakerProviderTIMEOUT() throws Exception
  {
    final TierLocalTaskRunnerConfig config = new TierLocalTaskRunnerConfig();
    final DeadhandResource resource = EasyMock.createStrictMock(DeadhandResource.class);
    resource.waitForHeartbeat(EasyMock.anyLong());
    EasyMock.expectLastCall().once().andThrow(new TimeoutException("test exception")).once();
    EasyMock.replay(resource);
    final ParentMonitorInputStreamFakerProvider provider = new ParentMonitorInputStreamFakerProvider(
        resource,
        config
    );
    try (final InputStream stream = provider.get()) {
      Assert.assertEquals(0, stream.read());
      Assert.assertEquals(-1, stream.read());
    }
    EasyMock.verify(resource);
  }

  @Test
  public void testParentMonitorInputStreamFakerProviderINTERRUPT() throws Exception
  {
    expectedEx.expect(IOException.class);
    final TierLocalTaskRunnerConfig config = new TierLocalTaskRunnerConfig();
    final DeadhandResource resource = EasyMock.createStrictMock(DeadhandResource.class);
    resource.waitForHeartbeat(EasyMock.anyLong());
    EasyMock.expectLastCall().andThrow(new InterruptedException("test exception")).once();
    EasyMock.replay(resource);
    final ParentMonitorInputStreamFakerProvider provider = new ParentMonitorInputStreamFakerProvider(
        resource,
        config
    );
    try {
      provider.get().read();
    }
    finally {
      EasyMock.verify(resource);
    }
  }

  @After
  public void tearDown()
  {
    // Clear interrupt flag
    Thread.interrupted();
  }
}

