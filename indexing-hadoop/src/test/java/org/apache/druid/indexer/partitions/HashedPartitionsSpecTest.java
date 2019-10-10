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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HashedPartitionsSpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void havingTargetRowsPerSegmentOnly()
  {
    final HashedPartitionsSpec hadoopHashedPartitionsSpec = jsonReadWriteRead(
        "{"
        + "   \"targetRowsPerSegment\":100,"
        + "   \"type\":\"hashed\""
        + "}"
    );

    Assert.assertTrue("isDeterminingPartitions", hadoopHashedPartitionsSpec.needsDeterminePartitions(true));

    Assert.assertNotNull(hadoopHashedPartitionsSpec.getMaxRowsPerSegment());
    Assert.assertEquals(
        "getMaxRowsPerSegment",
        100,
        hadoopHashedPartitionsSpec.getMaxRowsPerSegment().intValue()
    );
    Assert.assertNull(hadoopHashedPartitionsSpec.getNumShards());
    Assert.assertEquals(
        "getPartitionDimensions",
        ImmutableList.of(),
        hadoopHashedPartitionsSpec.getPartitionDimensions()
    );
  }

  @Test
  public void havingNumShardsOnly()
  {
    final HashedPartitionsSpec hadoopHashedPartitionsSpec = jsonReadWriteRead(
        "{"
        + "   \"type\":\"hashed\","
        + "   \"numShards\":2"
        + "}"
    );

    Assert.assertFalse("isDeterminingPartitions", hadoopHashedPartitionsSpec.needsDeterminePartitions(true));

    Assert.assertNull(
        "getMaxRowsPerSegment",
        hadoopHashedPartitionsSpec.getMaxRowsPerSegment()
    );

    Assert.assertNotNull(hadoopHashedPartitionsSpec.getNumShards());
    Assert.assertEquals(
        "shardCount",
        2,
        hadoopHashedPartitionsSpec.getNumShards().intValue()
    );

    Assert.assertEquals(
        "getPartitionDimensions",
        ImmutableList.of(),
        hadoopHashedPartitionsSpec.getPartitionDimensions()
    );
  }

  @Test
  public void havingIncompatiblePropertiesIsForbidden()
  {
    final String targetRowsPerSegment = DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT;
    final String targetPartitionSize = DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE;
    final String maxRowsPerSegment = PartitionsSpec.MAX_ROWS_PER_SEGMENT;
    final String numShards = HashedPartitionsSpec.NUM_SHARDS;

    Multimap<String, String> incompatiblePairs = ImmutableMultimap.<String, String>builder()
        .put(targetRowsPerSegment, targetPartitionSize)
        .put(targetRowsPerSegment, maxRowsPerSegment)
        .put(targetRowsPerSegment, numShards)
        .put(targetPartitionSize, maxRowsPerSegment)
        .put(targetPartitionSize, numShards)
        .put(maxRowsPerSegment, numShards)
        .build();

    for (Map.Entry<String, String> test : incompatiblePairs.entries()) {
      String first = test.getKey();
      String second = test.getValue();
      String reasonPrefix = first + "/" + second;

      String json = "{"
                    + "\"type\":\"hashed\""
                    + ",\"" + first + "\":100"
                    + ",\"" + second + "\":100"
                    + "}";
      try {
        jsonReadWriteRead(json);
        Assert.fail(reasonPrefix + " did not throw exception");
      }
      catch (RuntimeException e) {
        final String expectedMessage = StringUtils.format(
            "At most one of [Property{name='%s', value=100}] or [Property{name='%s', value=100}] must be present",
            first,
            second
        );
        Assert.assertThat(
            reasonPrefix + " has wrong failure message",
            e.getMessage(),
            CoreMatchers.containsString(expectedMessage)
        );
      }
    }
  }

  @Test
  public void defaults()
  {
    final HashedPartitionsSpec spec = jsonReadWriteRead("{\"type\":\"hashed\"}");
    Assert.assertNotNull(spec.getMaxRowsPerSegment());
    Assert.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());
    Assert.assertNull(spec.getNumShards());
    Assert.assertEquals(Collections.emptyList(), spec.getPartitionDimensions());
  }

  @Test
  public void handlesHistoricalNull()
  {
    String json = "{"
                  + "\"type\":\"hashed\""
                  + ",\"targetRowsPerSegment\":" + PartitionsSpec.HISTORICAL_NULL
                  + ",\"numShards\":" + PartitionsSpec.HISTORICAL_NULL
                  + "}";
    final HashedPartitionsSpec spec = jsonReadWriteRead(json);
    Assert.assertNotNull(spec.getMaxRowsPerSegment());
    Assert.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());
    Assert.assertNull(spec.getNumShards());
    Assert.assertEquals(Collections.emptyList(), spec.getPartitionDimensions());
  }

  @Test
  public void failsIfNotPositive()
  {
    List<String> properties = ImmutableList.of(
        DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE,
        PartitionsSpec.MAX_ROWS_PER_SEGMENT,
        HashedPartitionsSpec.NUM_SHARDS
    );

    for (String property : properties) {
      String json = "{"
                    + "\"type\":\"hashed\""
                    + ",\"" + property + "\":0"
                    + "}";
      try {
        jsonReadWriteRead(json);
        Assert.fail(property + " did not throw exception");
      }
      catch (RuntimeException e) {
        String expectedMessage = property + "[0] should be positive";
        Assert.assertThat(
            property + " has wrong failure message",
            e.getMessage(),
            CoreMatchers.containsString(expectedMessage)
        );
      }
    }
  }

  @Test
  public void backwardCompatibleWithTargetPartitionSize()
  {
    String json = "{"
                  + "\"type\":\"hashed\""
                  + ",\"targetPartitionSize\":100"
                  + "}";
    HashedPartitionsSpec hadoopHashedPartitionsSpec = jsonReadWriteRead(json);

    Assert.assertNotNull(hadoopHashedPartitionsSpec.getMaxRowsPerSegment());
    Assert.assertEquals(
        "getMaxRowsPerSegment",
        100,
        hadoopHashedPartitionsSpec.getMaxRowsPerSegment().intValue()
    );
  }

  @Test
  public void backwardCompatibleWithMaxRowsPerSegment()
  {
    String json = "{"
                  + "\"type\":\"hashed\""
                  + ",\"maxRowsPerSegment\":100"
                  + "}";
    HashedPartitionsSpec hadoopHashedPartitionsSpec = jsonReadWriteRead(json);

    Assert.assertNotNull(hadoopHashedPartitionsSpec.getMaxRowsPerSegment());
    Assert.assertEquals(
        "getMaxRowsPerSegment",
        100,
        hadoopHashedPartitionsSpec.getMaxRowsPerSegment().intValue()
    );
  }

  private static HashedPartitionsSpec jsonReadWriteRead(String s)
  {
    try {
      byte[] jsonBytes = JSON_MAPPER.writeValueAsBytes(JSON_MAPPER.readValue(s, PartitionsSpec.class));
      PartitionsSpec partitionsSpec = JSON_MAPPER.readValue(jsonBytes, PartitionsSpec.class);
      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
      return (HashedPartitionsSpec) partitionsSpec;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
