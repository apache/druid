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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class GroupByQueryConfigTest
{
  private final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private final ImmutableMap<String, String> CONFIG_MAP = ImmutableMap
      .<String, String>builder()
      .put("singleThreaded", "true")
      .put("defaultStrategy", "v2")
      .put("bufferGrouperInitialBuckets", "1")
      .put("maxIntermediateRows", "2")
      .put("maxResults", "3")
      .put("maxOnDiskStorage", "4")
      .put("maxMergingDictionarySize", "5")
      .put("bufferGrouperMaxLoadFactor", "6")
      .build();

  @Test
  public void testSerde()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(CONFIG_MAP, GroupByQueryConfig.class);

    Assert.assertEquals(true, config.isSingleThreaded());
    Assert.assertEquals("v2", config.getDefaultStrategy());
    Assert.assertEquals(1, config.getBufferGrouperInitialBuckets());
    Assert.assertEquals(2, config.getMaxIntermediateRows());
    Assert.assertEquals(3, config.getMaxResults());
    Assert.assertEquals(4, config.getMaxOnDiskStorage());
    Assert.assertEquals(5, config.getMaxMergingDictionarySize());
    Assert.assertEquals(6.0, config.getBufferGrouperMaxLoadFactor(), 0.0);
  }

  @Test
  public void testNoOverrides()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(CONFIG_MAP, GroupByQueryConfig.class);
    final GroupByQueryConfig config2 = config.withOverrides(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.of("2000/P1D"))
                    .setGranularity(Granularities.ALL)
                    .build()
    );

    Assert.assertEquals(true, config2.isSingleThreaded());
    Assert.assertEquals("v2", config2.getDefaultStrategy());
    Assert.assertEquals(1, config2.getBufferGrouperInitialBuckets());
    Assert.assertEquals(2, config2.getMaxIntermediateRows());
    Assert.assertEquals(3, config2.getMaxResults());
    Assert.assertEquals(4, config2.getMaxOnDiskStorage());
    Assert.assertEquals(5, config2.getMaxMergingDictionarySize());
    Assert.assertEquals(6.0, config2.getBufferGrouperMaxLoadFactor(), 0.0);
  }

  @Test
  public void testOverrides()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(CONFIG_MAP, GroupByQueryConfig.class);
    final GroupByQueryConfig config2 = config.withOverrides(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.of("2000/P1D"))
                    .setGranularity(Granularities.ALL)
                    .setContext(
                        ImmutableMap.<String, Object>of(
                            "groupByStrategy", "v1",
                            "maxOnDiskStorage", 0,
                            "maxResults", 2,
                            "maxMergingDictionarySize", 3
                        )
                    )
                    .build()
    );

    Assert.assertEquals(true, config2.isSingleThreaded());
    Assert.assertEquals("v1", config2.getDefaultStrategy());
    Assert.assertEquals(1, config2.getBufferGrouperInitialBuckets());
    Assert.assertEquals(2, config2.getMaxIntermediateRows());
    Assert.assertEquals(2, config2.getMaxResults());
    Assert.assertEquals(0, config2.getMaxOnDiskStorage());
    Assert.assertEquals(3, config2.getMaxMergingDictionarySize());
    Assert.assertEquals(6.0, config2.getBufferGrouperMaxLoadFactor(), 0.0);
  }
}
