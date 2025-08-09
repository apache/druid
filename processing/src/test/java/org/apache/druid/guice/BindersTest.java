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
import com.google.inject.name.Names;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class BindersTest
{

  @Test
  public void test_bindTaskLogs()

  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.logs.type", "noop");
    
    Injector injector = Guice.createInjector(
        binder -> {
          binder.bind(Properties.class).toInstance(props);
          PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), null);
          PolyBind.createChoiceWithDefault(binder, "druid.indexer.logs.defaultType", Key.get(TaskLogs.class, Names.named("switching.defaultType")), "noop");
          Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
        }
    );

    TaskLogs taskLogs = injector.getInstance(TaskLogs.class);
    Assertions.assertInstanceOf(NoopTaskLogs.class, taskLogs);

    TaskLogs defaultTypeTaskLogs = injector.getInstance(Key.get(TaskLogs.class, Names.named("switching.defaultType")));
    Assertions.assertInstanceOf(NoopTaskLogs.class, defaultTypeTaskLogs);
  }
}
