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

package org.apache.druid.k8s.overlord.common.httpclient.okhttp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DruidKubernetesOkHttpHttpClientConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerdeWithDefaults() throws IOException
  {
    String json = "{}";
    DruidKubernetesOkHttpHttpClientConfig config = OBJECT_MAPPER.readValue(
        json,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertTrue(config.isUseCustomDispatcherExecutor());
    Assert.assertEquals(50, config.getCoreWorkerThreads());
    Assert.assertEquals(50, config.getMaxWorkerThreads());
    Assert.assertEquals(60L, config.getWorkerThreadKeepAliveTime());

    String serialized = OBJECT_MAPPER.writeValueAsString(config);
    DruidKubernetesOkHttpHttpClientConfig deserialized = OBJECT_MAPPER.readValue(
        serialized,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertEquals(config.isUseCustomDispatcherExecutor(), deserialized.isUseCustomDispatcherExecutor());
    Assert.assertEquals(config.getCoreWorkerThreads(), deserialized.getCoreWorkerThreads());
    Assert.assertEquals(config.getMaxWorkerThreads(), deserialized.getMaxWorkerThreads());
    Assert.assertEquals(config.getWorkerThreadKeepAliveTime(), deserialized.getWorkerThreadKeepAliveTime());
  }

  @Test
  public void testSerdeWithAllFieldsSet() throws IOException
  {
    String json = "{\n"
                  + "  \"useCustomDispatcherExecutor\": false,\n"
                  + "  \"maxWorkerThreads\": 99,\n"
                  + "  \"coreWorkerThreads\": 30,\n"
                  + "  \"workerThreadKeepAliveTime\": 120\n"
                  + "}";

    DruidKubernetesOkHttpHttpClientConfig config = OBJECT_MAPPER.readValue(
        json,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertFalse(config.isUseCustomDispatcherExecutor());
    Assert.assertEquals(30, config.getCoreWorkerThreads());
    Assert.assertEquals(99, config.getMaxWorkerThreads());
    Assert.assertEquals(120L, config.getWorkerThreadKeepAliveTime());

    String serialized = OBJECT_MAPPER.writeValueAsString(config);
    DruidKubernetesOkHttpHttpClientConfig deserialized = OBJECT_MAPPER.readValue(
        serialized,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertEquals(config.isUseCustomDispatcherExecutor(), deserialized.isUseCustomDispatcherExecutor());
    Assert.assertEquals(config.getCoreWorkerThreads(), deserialized.getCoreWorkerThreads());
    Assert.assertEquals(config.getMaxWorkerThreads(), deserialized.getMaxWorkerThreads());
    Assert.assertEquals(config.getWorkerThreadKeepAliveTime(), deserialized.getWorkerThreadKeepAliveTime());
  }

  @Test
  public void testSerdeWithNullMaxWorkerThreads() throws IOException
  {
    String json = "{\n"
                  + "  \"useCustomDispatcherExecutor\": true,\n"
                  + "  \"maxWorkerThreads\": null,\n"
                  + "  \"coreWorkerThreads\": 40,\n"
                  + "  \"workerThreadKeepAliveTime\": 90\n"
                  + "}";

    DruidKubernetesOkHttpHttpClientConfig config = OBJECT_MAPPER.readValue(
        json,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertTrue(config.isUseCustomDispatcherExecutor());
    Assert.assertEquals(40, config.getCoreWorkerThreads());
    Assert.assertEquals(40, config.getMaxWorkerThreads());
    Assert.assertEquals(90L, config.getWorkerThreadKeepAliveTime());

    String serialized = OBJECT_MAPPER.writeValueAsString(config);
    DruidKubernetesOkHttpHttpClientConfig deserialized = OBJECT_MAPPER.readValue(
        serialized,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertEquals(config.isUseCustomDispatcherExecutor(), deserialized.isUseCustomDispatcherExecutor());
    Assert.assertEquals(config.getCoreWorkerThreads(), deserialized.getCoreWorkerThreads());
    Assert.assertEquals(config.getMaxWorkerThreads(), deserialized.getMaxWorkerThreads());
    Assert.assertEquals(config.getWorkerThreadKeepAliveTime(), deserialized.getWorkerThreadKeepAliveTime());
  }

  @Test
  public void testMaxWorkerThreadsLogic()
  {
    DruidKubernetesOkHttpHttpClientConfig config = new DruidKubernetesOkHttpHttpClientConfig();

    Assert.assertEquals(50, config.getMaxWorkerThreads());
  }

  @Test
  public void testMaxWorkerThreadsGreaterThanCore() throws IOException
  {
    String json = "{\n"
                  + "  \"maxWorkerThreads\": 100,\n"
                  + "  \"coreWorkerThreads\": 30\n"
                  + "}";

    DruidKubernetesOkHttpHttpClientConfig config = OBJECT_MAPPER.readValue(
        json,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertEquals(30, config.getCoreWorkerThreads());
    Assert.assertEquals(100, config.getMaxWorkerThreads());
  }

  @Test
  public void testMaxWorkerThreadsLessThanCore() throws IOException
  {
    String json = "{\n"
                  + "  \"maxWorkerThreads\": 20,\n"
                  + "  \"coreWorkerThreads\": 30\n"
                  + "}";

    DruidKubernetesOkHttpHttpClientConfig config = OBJECT_MAPPER.readValue(
        json,
        DruidKubernetesOkHttpHttpClientConfig.class
    );

    Assert.assertEquals(30, config.getCoreWorkerThreads());
    Assert.assertEquals(30, config.getMaxWorkerThreads());
  }
}