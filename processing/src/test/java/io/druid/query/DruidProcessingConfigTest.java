/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.config.Config;
import org.junit.Assert;
import org.junit.Test;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

/**
 */
public class DruidProcessingConfigTest
{

  @Test
  public void testDeserialization() throws Exception
  {
    ConfigurationObjectFactory factory = Config.createFactory(new Properties());

    //with defaults
    DruidProcessingConfig config = factory.build(DruidProcessingConfig.class);

    Assert.assertEquals(1024 * 1024 * 1024, config.intermediateComputeSizeBytes());
    Assert.assertEquals(Integer.MAX_VALUE, config.poolCacheMaxCount());
    if (Runtime.getRuntime().availableProcessors() == 1) {
      Assert.assertTrue(config.getNumThreads() == 1);
    } else {
      Assert.assertTrue(config.getNumThreads() < Runtime.getRuntime().availableProcessors());
    }
    Assert.assertEquals(0, config.columnCacheSizeBytes());
    Assert.assertFalse(config.isFifo());

    //with non-defaults
    Properties props = new Properties();
    props.setProperty("druid.processing.buffer.sizeBytes", "1");
    props.setProperty("druid.processing.buffer.poolCacheMaxCount", "1");
    props.setProperty("druid.processing.numThreads", "5");
    props.setProperty("druid.processing.columnCache.sizeBytes", "1");
    props.setProperty("druid.processing.fifo", "true");

    factory = Config.createFactory(props);
    config = factory.buildWithReplacements(DruidProcessingConfig.class, ImmutableMap.of("base_path", "druid.processing"));

    Assert.assertEquals(1, config.intermediateComputeSizeBytes());
    Assert.assertEquals(1, config.poolCacheMaxCount());
    Assert.assertEquals(5, config.getNumThreads());
    Assert.assertEquals(1, config.columnCacheSizeBytes());
    Assert.assertTrue(config.isFifo());
  }
}
