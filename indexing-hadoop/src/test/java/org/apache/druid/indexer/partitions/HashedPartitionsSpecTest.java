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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HashedPartitionsSpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testHashedPartitionsSpec()
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

    Assert.assertEquals(
        "getPartitionDimensions",
        ImmutableList.of(),
        hadoopHashedPartitionsSpec.getPartitionDimensions()
    );
  }

  @Test
  public void testHashedPartitionsSpecShardCount()
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
  public void testHashedPartitionsSpecBothTargetForbidden()
  {
    exception.expect(RuntimeException.class);
    exception.expectMessage("At most one of targetRowsPerSegment or targetPartitionSize must be present");

    String json = "{"
                  + "\"type\":\"hashed\""
                  + ",\"targetRowsPerSegment\":100"
                  + ",\"targetPartitionSize\":100"
                  + "}";
    jsonReadWriteRead(json);
  }

  @Test
  public void testHashedPartitionsSpecBackwardCompatibleTargetPartitionSize()
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
  public void testHashedPartitionsSpecBackwardCompatibleMaxRowsPerSegment()
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
