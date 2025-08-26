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

package org.apache.druid.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.SwitchingTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class IndexingServiceTaskLogsModuleTest
{

  @Test
  public void test_switchingTaskLogs_usesDefaultType_whenNoPusherConfigured()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
        },
        new ConfigModule(),
        new JacksonModule(),
        new IndexingServiceTaskLogsModule(props)
    );

    TaskLogs taskLogs = injector.getInstance(TaskLogs.class);
    Assert.assertTrue(taskLogs instanceof SwitchingTaskLogs);

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.logPushType")));
    Assert.assertTrue(pusher instanceof FileTaskLogs);
    TaskLogs reports = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.reportsType")));
    Assert.assertTrue(reports instanceof FileTaskLogs);
    TaskLogs stream = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.logStreamType")));
    Assert.assertTrue(stream instanceof FileTaskLogs);
  }

  @Test
  public void test_switchingTaskLogs_usesConfiguredPusherType()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");
    props.setProperty("druid.indexer.logs.switching.logPushType", "file");
    props.setProperty("druid.indexer.logs.switching.reportsType", "noop");
    props.setProperty("druid.indexer.logs.switching.logStreamType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
        },
        new ConfigModule(),
        new IndexingServiceTaskLogsModule(props),
        new JacksonModule()
    );

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.logPushType")));
    Assert.assertTrue(pusher instanceof FileTaskLogs);
    TaskLogs reports = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.reportsType")));
    Assert.assertTrue(reports instanceof NoopTaskLogs);
    TaskLogs stream = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.logStreamType")));
    Assert.assertTrue(stream instanceof FileTaskLogs);
  }
}
