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
import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class HttpEmitterConfigTest
{
  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    props.put("org.apache.druid.java.util.emitter.recipientBaseUrl", "http://example.com/");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        HttpEmitterConfig.class
    );

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(500, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertNull(config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Pair<Integer, Integer> batchConfigPair = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        Runtime.getRuntime().maxMemory()
    );
    Assert.assertEquals(batchConfigPair.lhs.intValue(), config.getMaxBatchSize());
    Assert.assertEquals(batchConfigPair.rhs.intValue(), config.getBatchQueueSizeLimit());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
    Assert.assertEquals(2.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(0, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testDefaultsLegacy()
  {
    final Properties props = new Properties();
    props.put("org.apache.druid.java.util.emitter.http.url", "http://example.com/");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(Emitters.makeHttpMap(props), HttpEmitterConfig.class);

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(300, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertNull(config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Pair<Integer, Integer> batchConfigPair = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        Runtime.getRuntime().maxMemory()
    );
    Assert.assertEquals(batchConfigPair.lhs.intValue(), config.getMaxBatchSize());
    Assert.assertEquals(batchConfigPair.rhs.intValue(), config.getBatchQueueSizeLimit());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
    Assert.assertEquals(2.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(0, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.flushMillis", "1");
    props.setProperty("org.apache.druid.java.util.emitter.flushCount", "2");
    props.setProperty("org.apache.druid.java.util.emitter.recipientBaseUrl", "http://example.com/");
    props.setProperty("org.apache.druid.java.util.emitter.basicAuthentication", "a:b");
    props.setProperty("org.apache.druid.java.util.emitter.batchingStrategy", "NEWLINES");
    props.setProperty("org.apache.druid.java.util.emitter.maxBatchSize", "4");
    props.setProperty("org.apache.druid.java.util.emitter.flushTimeOut", "1000");
    props.setProperty("org.apache.druid.java.util.emitter.batchQueueSizeLimit", "2500");
    props.setProperty("org.apache.druid.java.util.emitter.httpTimeoutAllowanceFactor", "3.0");
    props.setProperty("org.apache.druid.java.util.emitter.minHttpTimeoutMillis", "100");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(
        Emitters.makeCustomFactoryMap(props),
        HttpEmitterConfig.class
    );

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication().getPassword());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
    Assert.assertEquals(2500, config.getBatchQueueSizeLimit());
    Assert.assertEquals(3.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(100, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testSettingEverythingLegacy()
  {
    final Properties props = new Properties();
    props.setProperty("org.apache.druid.java.util.emitter.flushMillis", "1");
    props.setProperty("org.apache.druid.java.util.emitter.flushCount", "2");
    props.setProperty("org.apache.druid.java.util.emitter.http.url", "http://example.com/");
    props.setProperty("org.apache.druid.java.util.emitter.http.basicAuthentication", "a:b");
    props.setProperty("org.apache.druid.java.util.emitter.http.batchingStrategy", "newlines");
    props.setProperty("org.apache.druid.java.util.emitter.http.maxBatchSize", "4");
    props.setProperty("org.apache.druid.java.util.emitter.http.flushTimeOut", "1000");
    props.setProperty("org.apache.druid.java.util.emitter.http.batchQueueSizeLimit", "2500");
    props.setProperty("org.apache.druid.java.util.emitter.http.httpTimeoutAllowanceFactor", "3.0");
    props.setProperty("org.apache.druid.java.util.emitter.http.minHttpTimeoutMillis", "100");

    final ObjectMapper objectMapper = new ObjectMapper();
    final HttpEmitterConfig config = objectMapper.convertValue(Emitters.makeHttpMap(props), HttpEmitterConfig.class);

    Assert.assertEquals(1, config.getFlushMillis());
    Assert.assertEquals(2, config.getFlushCount());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertEquals("a:b", config.getBasicAuthentication().getPassword());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(4, config.getMaxBatchSize());
    Assert.assertEquals(1000, config.getFlushTimeOut());
    Assert.assertEquals(2500, config.getBatchQueueSizeLimit());
    Assert.assertEquals(3.0f, config.getHttpTimeoutAllowanceFactor(), 0.0f);
    Assert.assertEquals(100, config.getMinHttpTimeoutMillis());
  }

  @Test
  public void testMemoryLimits()
  {
    Pair<Integer, Integer> batchConfigPair = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        64 * 1024 * 1024
    );
    Assert.assertEquals(3355443, batchConfigPair.lhs.intValue());
    Assert.assertEquals(2, batchConfigPair.rhs.intValue());

    Pair<Integer, Integer> batchConfigPair2 = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        128 * 1024 * 1024
    );
    Assert.assertEquals(5242880, batchConfigPair2.lhs.intValue());
    Assert.assertEquals(2, batchConfigPair2.rhs.intValue());

    Pair<Integer, Integer> batchConfigPair3 = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        256 * 1024 * 1024
    );
    Assert.assertEquals(5242880, batchConfigPair3.lhs.intValue());
    Assert.assertEquals(5, batchConfigPair3.rhs.intValue());

    Pair<Integer, Integer> batchConfigPair4 = BaseHttpEmittingConfig.getDefaultBatchSizeAndLimit(
        Long.MAX_VALUE
    );
    Assert.assertEquals(5242880, batchConfigPair4.lhs.intValue());
    Assert.assertEquals(50, batchConfigPair4.rhs.intValue());
  }
}
