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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SegmentLoaderConfigTest
{
  @Test
  void testSetVirtualStorage()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig();

    // Verify default values
    Assertions.assertFalse(config.isVirtualStorage());
    Assertions.assertFalse(config.isVirtualStorageEphemeral());

    // Set both to true
    config.setVirtualStorage(true).setVirtualStorageIsEphemeral(true);

    // Verify both fields are set
    Assertions.assertTrue(config.isVirtualStorage());
    Assertions.assertTrue(config.isVirtualStorageEphemeral());
  }

  @Test
  void testVirtualStorageCoalesceGapBytes() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));

    // default
    final SegmentLoaderConfig defaults = jsonMapper.readValue("{}", SegmentLoaderConfig.class);
    Assertions.assertEquals(1024L * 1024L, defaults.getVirtualStorageCoalesceGapBytes());

    // configured
    final SegmentLoaderConfig configured = jsonMapper.readValue(
        "{\"virtualStorageCoalesceGapBytes\": 65536}",
        SegmentLoaderConfig.class
    );
    Assertions.assertEquals(65536L, configured.getVirtualStorageCoalesceGapBytes());
  }

  @Test
  void testVirtualStorageMaxFetchRunBytes() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(RuntimeInfo.class, new RuntimeInfo()));

    // default
    final SegmentLoaderConfig defaults = jsonMapper.readValue("{}", SegmentLoaderConfig.class);
    Assertions.assertEquals(64L * 1024L * 1024L, defaults.getVirtualStorageMaxFetchRunBytes());

    // configured
    final SegmentLoaderConfig configured = jsonMapper.readValue(
        "{\"virtualStorageMaxFetchRunBytes\": 8388608}",
        SegmentLoaderConfig.class
    );
    Assertions.assertEquals(8388608L, configured.getVirtualStorageMaxFetchRunBytes());
  }
}
