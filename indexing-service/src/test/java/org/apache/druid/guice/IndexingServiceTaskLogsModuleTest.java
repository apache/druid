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
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogs;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class IndexingServiceTaskLogsModuleTest
{

  @Test
  public void test_switchingTaskLogs_shouldUseDefaultWhenNoPusherConfigured()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule(props),
        new JacksonModule()
    );

    TaskLogs taskLogs = injector.getInstance(TaskLogs.class);
    Assert.assertTrue(taskLogs instanceof SwitchingTaskLogs);
  }

  @Test
  public void test_providePusher_shouldReturnConfiguredImplementaions()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "switching");
    props.setProperty("druid.indexer.logs.switching.defaultType", "noop");
    props.setProperty("druid.indexer.logs.switching.pushType", "file");
    props.setProperty("druid.indexer.logs.switching.reportsType", "noop");
    props.setProperty("druid.indexer.logs.switching.streamType", "file");

    Injector injector = Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(Validator.class).toInstance(new MockValidator());
        },
        new IndexingServiceTaskLogsModule(props),
        new JacksonModule()
    );

    TaskLogs pusher = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.pushType")));
    Assert.assertTrue(pusher instanceof FileTaskLogs);
    TaskLogs reports = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.reportType")));
    Assert.assertTrue(reports instanceof NoopTaskLogs);
    TaskLogs stream = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.streamType")));
    Assert.assertTrue(stream instanceof FileTaskLogs);
  }

  private static class MockValidator implements Validator
  {
    @Override
    public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
    {
      return Collections.emptySet();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateProperty(
        T object,
        String propertyName,
        Class<?>... groups
    )
    {
      return Collections.emptySet();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateValue(
        Class<T> beanType,
        String propertyName,
        Object value,
        Class<?>... groups
    )
    {
      return Collections.emptySet();
    }

    @Override
    public BeanDescriptor getConstraintsForClass(Class<?> clazz)
    {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> type)
    {
      return null;
    }

    @Override
    public ExecutableValidator forExecutables()
    {
      return null;
    }
  }
}
