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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupByQueryConfigTest
{
  private final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private final ImmutableMap<String, String> CONFIG_MAP = ImmutableMap
      .<String, String>builder()
      .put("singleThreaded", "true")
      .put("bufferGrouperInitialBuckets", "1")
      .put("defaultOnDiskStorage", "1M")
      .put("maxOnDiskStorage", "4M")
      .put("maxSelectorDictionarySize", "5")
      .put("maxMergingDictionarySize", "6M")
      .put("bufferGrouperMaxLoadFactor", "7")
      .put("maxSpillFileCount", "123")
      .build();

  @Test
  public void testSerde()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(CONFIG_MAP, GroupByQueryConfig.class);

    Assertions.assertEquals(true, config.isSingleThreaded());
    Assertions.assertEquals(1, config.getBufferGrouperInitialBuckets());
    Assertions.assertEquals(4_000_000, config.getMaxOnDiskStorage().getBytes());
    Assertions.assertEquals(123, config.getMaxSpillFileCount());
    Assertions.assertEquals(1_000_000, config.getDefaultOnDiskStorage().getBytes());
    Assertions.assertEquals(5, config.getConfiguredMaxSelectorDictionarySize());
    Assertions.assertEquals(6_000_000, config.getConfiguredMaxMergingDictionarySize());
    Assertions.assertEquals(7.0, config.getBufferGrouperMaxLoadFactor(), 0.0);
    Assertions.assertFalse(config.isApplyLimitPushDownToSegment());
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

    Assertions.assertEquals(true, config2.isSingleThreaded());
    Assertions.assertEquals(1, config2.getBufferGrouperInitialBuckets());
    Assertions.assertEquals(1_000_000, config2.getMaxOnDiskStorage().getBytes());
    Assertions.assertEquals(123, config2.getMaxSpillFileCount());
    Assertions.assertEquals(5, config2.getConfiguredMaxSelectorDictionarySize());
    Assertions.assertEquals(6_000_000, config2.getConfiguredMaxMergingDictionarySize());
    Assertions.assertEquals(7.0, config2.getBufferGrouperMaxLoadFactor(), 0.0);
    Assertions.assertEquals(DeferExpressionDimensions.FIXED_WIDTH_NON_NUMERIC, config2.getDeferExpressionDimensions());
    Assertions.assertFalse(config2.isApplyLimitPushDownToSegment());
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
                        ImmutableMap.<String, Object>builder()
                                    .put("maxOnDiskStorage", "3M")
                                    .put("maxResults", 2)
                                    .put("maxSelectorDictionarySize", 3)
                                    .put("maxMergingDictionarySize", 4)
                                    .put("maxSpillFileCount", 333)
                                    .put("applyLimitPushDownToSegment", true)
                                    .put(
                                        GroupByQueryConfig.CTX_KEY_DEFER_EXPRESSION_DIMENSIONS,
                                        DeferExpressionDimensions.ALWAYS.toString()
                                    )
                                    .build()
                    )
                    .build()
    );

    Assertions.assertEquals(true, config2.isSingleThreaded());
    Assertions.assertEquals(1, config2.getBufferGrouperInitialBuckets());
    Assertions.assertEquals(3_000_000, config2.getMaxOnDiskStorage().getBytes());
    Assertions.assertEquals(333, config2.getMaxSpillFileCount());
    Assertions.assertEquals(3, config2.getConfiguredMaxSelectorDictionarySize());
    Assertions.assertEquals(4, config2.getConfiguredMaxMergingDictionarySize());
    Assertions.assertEquals(7.0, config2.getBufferGrouperMaxLoadFactor(), 0.0);
    Assertions.assertEquals(DeferExpressionDimensions.ALWAYS, config2.getDeferExpressionDimensions());
    Assertions.assertTrue(config2.isApplyLimitPushDownToSegment());
  }

  @Test
  public void testAutomaticMergingDictionarySize()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxMergingDictionarySize", "0"),
        GroupByQueryConfig.class
    );

    Assertions.assertEquals(GroupByQueryConfig.AUTOMATIC, config.getConfiguredMaxMergingDictionarySize());
    Assertions.assertEquals(150_000_000, config.getActualMaxMergingDictionarySize(1_000_000_000, 2));
  }

  @Test
  public void testNonAutomaticMergingDictionarySize()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxMergingDictionarySize", "100"),
        GroupByQueryConfig.class
    );

    Assertions.assertEquals(100, config.getConfiguredMaxMergingDictionarySize());
    Assertions.assertEquals(100, config.getActualMaxMergingDictionarySize(1_000_000_000, 2));
  }

  @Test
  public void testAutomaticSelectorDictionarySize()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxSelectorDictionarySize", "0"),
        GroupByQueryConfig.class
    );

    Assertions.assertEquals(GroupByQueryConfig.AUTOMATIC, config.getConfiguredMaxSelectorDictionarySize());
    Assertions.assertEquals(50_000_000, config.getActualMaxSelectorDictionarySize(1_000_000_000, 2));
  }

  @Test
  public void testNonAutomaticSelectorDictionarySize()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxSelectorDictionarySize", "100"),
        GroupByQueryConfig.class
    );

    Assertions.assertEquals(100, config.getConfiguredMaxSelectorDictionarySize());
    Assertions.assertEquals(100, config.getActualMaxSelectorDictionarySize(1_000_000_000, 2));
  }

  /**
   * Tests that the defaultOnDiskStorage value is used when applying override context that is lacking maxOnDiskStorage.
   */
  @Test
  public void testUseDefaultOnDiskStorage()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of(
            "maxOnDiskStorage", "10G",
            "defaultOnDiskStorage", "5G"
        ),
        GroupByQueryConfig.class
    );
    final GroupByQueryConfig config2 = config.withOverrides(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.of("2000/P1D"))
                    .setGranularity(Granularities.ALL)
                    .setContext(ImmutableMap.<String, Object>builder().build())
                    .build()
    );
    Assertions.assertEquals(5_000_000_000L, config2.getMaxOnDiskStorage().getBytes());
  }

  @Test
  public void testUseMaxOnDiskStorageWhenClientOverrideIsTooLarge()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxOnDiskStorage", "500M"),
        GroupByQueryConfig.class
    );
    final GroupByQueryConfig config2 = config.withOverrides(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setInterval(Intervals.of("2000/P1D"))
                    .setGranularity(Granularities.ALL)
                    .setContext(
                        ImmutableMap.<String, Object>builder()
                            .put("maxOnDiskStorage", "1G")
                            .build()
                    )
                    .build()
    );
    Assertions.assertEquals(500_000_000, config2.getMaxOnDiskStorage().getBytes());
  }

  @Test
  public void testGetDefaultOnDiskStorageReturnsCorrectValue()
  {
    final GroupByQueryConfig config = MAPPER.convertValue(
        ImmutableMap.of("maxOnDiskStorage", "500M"),
        GroupByQueryConfig.class
    );
    final GroupByQueryConfig config2 = MAPPER.convertValue(
        ImmutableMap.of("maxOnDiskStorage", "500M",
                        "defaultOnDiskStorage", "100M"),
        GroupByQueryConfig.class
    );
    Assertions.assertEquals(500_000_000, config.getDefaultOnDiskStorage().getBytes());
    Assertions.assertEquals(100_000_000, config2.getDefaultOnDiskStorage().getBytes());
  }
}
