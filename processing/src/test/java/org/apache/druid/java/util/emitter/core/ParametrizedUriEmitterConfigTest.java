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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.utils.JvmUtils;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ParametrizedUriEmitterConfigTest
{
  /**
   * The JVM RuntimeInfo is static, and the default settings that depend on it such
   * as {@link BaseHttpEmittingConfig#DEFAULT_BATCH_QUEUE_SIZE_LIMIT} are static as well. So bind the runtime info
   * for this test, so it doesn't get pawned by other tests setting the runtime to different values.
   */
  private static Injector makeInjector(Properties props)
  {
    return Guice.createInjector(
        binder -> {
          JvmUtils.resetTestsToDefaultRuntimeInfo();
          binder.bind(RuntimeInfo.class)
                .toInstance(JvmUtils.getRuntimeInfo());
          binder.requestStaticInjection(JvmUtils.class);

          final ParametrizedUriEmitterConfig paramConfig = new ObjectMapper().convertValue(
              Emitters.makeCustomFactoryMap(props), ParametrizedUriEmitterConfig.class);
          final HttpEmitterConfig httpEmitterConfig = paramConfig.buildHttpEmitterConfig("http://example.com/topic");
          binder.bind(HttpEmitterConfig.class).toInstance(httpEmitterConfig);
        }
    );
  }

  @AfterClass
  public static void teardown()
  {
    JvmUtils.resetTestsToDefaultRuntimeInfo();
  }

  @Test
  public void testDefaults()
  {
    final Injector injector = makeInjector(new Properties());
    final HttpEmitterConfig config = injector.getInstance(HttpEmitterConfig.class);

    Assert.assertEquals(BaseHttpEmittingConfig.DEFAULT_MAX_BATCH_SIZE, config.getMaxBatchSize());
    Assert.assertEquals(BaseHttpEmittingConfig.DEFAULT_BATCH_QUEUE_SIZE_LIMIT, config.getBatchQueueSizeLimit());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.flushMillis", "1");
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.flushCount", "2");
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.basicAuthentication", "a:b");
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.batchingStrategy", "NEWLINES");
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.maxBatchSize", "4");
    props.setProperty("org.apache.druid.java.util.emitter.httpEmitting.flushTimeOut", "1000");

    final Injector injector = makeInjector(props);
    final HttpEmitterConfig config = injector.getInstance(HttpEmitterConfig.class);

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/topic", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication().getPassword());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
  }
}
