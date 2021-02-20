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

package org.apache.druid.k8s.middlemanager;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.Config;
import okhttp3.OkHttpClient;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.k8s.middlemanager.common.DefaultK8sApiClient;
import org.apache.druid.k8s.middlemanager.common.K8sApiClient;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogKiller;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class K8sMiddleManagerModule implements DruidModule
{
  private static final String INDEXER_RUNNER_MODE_K8S = "k8s";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.middlemanager.k8s", K8sMiddleManagerConfig.class);
    final MapBinder<String, TaskRunner> biddy = PolyBind.optionBinder(
            binder,
            Key.get(TaskRunner.class)
    );
    biddy.addBinding(INDEXER_RUNNER_MODE_K8S).to(K8sForkingTaskRunner.class).in(LazySingleton.class);
    binder.bind(K8sForkingTaskRunner.class).in(LazySingleton.class);
    configureTaskLogs(binder);

    bindK8sClient(binder);
  }

  private void bindK8sClient(Binder binder)
  {
    binder.bind(ApiClient.class)
            .toProvider(
                () -> {
                  try {
                    // Note: we can probably improve things here about figuring out how to find the K8S API server,
                    // HTTP client timeouts etc.
                    // Current is about never timeout.
                    ApiClient client = Config.defaultClient();
                    OkHttpClient httpClient =
                            client.getHttpClient()
                                    .newBuilder()
                                    .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                                    .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                                    .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                                    .build();
                    client.setHttpClient(httpClient);
                    Configuration.setDefaultApiClient(client);
                    return client;
                  }
                  catch (IOException ex) {
                    throw new RuntimeException("Failed to create K8s ApiClient instance", ex);
                  }
                }
            )
            .in(LazySingleton.class);
    binder.bind(K8sApiClient.class).to(DefaultK8sApiClient.class).in(LazySingleton.class);
  }

  private void configureTaskLogs(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
    JsonConfigProvider.bind(binder, "druid.indexer.logs", FileTaskLogsConfig.class);

    final MapBinder<String, TaskLogs> taskLogBinder = Binders.taskLogsBinder(binder);
    taskLogBinder.addBinding("noop").to(NoopTaskLogs.class).in(LazySingleton.class);
    taskLogBinder.addBinding("file").to(FileTaskLogs.class).in(LazySingleton.class);
    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(FileTaskLogs.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
    binder.bind(TaskLogKiller.class).to(TaskLogs.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }
}
