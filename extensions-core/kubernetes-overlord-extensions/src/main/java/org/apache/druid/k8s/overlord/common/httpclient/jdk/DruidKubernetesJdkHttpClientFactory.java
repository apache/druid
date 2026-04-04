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

package org.apache.druid.k8s.overlord.common.httpclient.jdk;

import io.fabric8.kubernetes.client.jdkhttp.JdkHttpClientFactory;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DruidKubernetesJdkHttpClientFactory extends JdkHttpClientFactory implements DruidKubernetesHttpClientFactory
{
  public static final String TYPE_NAME = "javaStandardHttp";
  private final DruidKubernetesJdkHttpClientConfig config;

  public DruidKubernetesJdkHttpClientFactory(DruidKubernetesJdkHttpClientConfig config)
  {
    super();
    this.config = config;
  }

  @Override
  protected HttpClient.Builder createNewHttpClientBuilder()
  {
    ExecutorService executorService = new ThreadPoolExecutor(
        config.getCoreWorkerThreads(),
        config.getMaxWorkerThreads(),
        config.getWorkerThreadKeepAliveTime(),
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        Execs.makeThreadFactory("JdkHttpClient-%d")
    );
    return HttpClient.newBuilder().executor(executorService);
  }

  @Override
  protected void additionalConfig(HttpClient.Builder builder)
  {
  }
}
