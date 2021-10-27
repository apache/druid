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

package org.apache.druid.timeline.partition;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.druid.timeline.partition.ShardSpecTestUtils.tupleOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiDimensionShardSpecTest
{

  private final List<String> dimensions = new ArrayList<>();

  @Test
  public void testIsInChunk()
  {
    setDimensions("d1", "d2");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        tupleOf("India", "Delhi"),
        tupleOf("Spain", "Valencia"),
        10,
        null
    );

    // Verify that entries starting from (India, Delhi) until (Spain, Valencia) are in chunk
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Delhi")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Kolkata")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Japan", "Tokyo")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Spain", "Barcelona")
    ));

    assertFalse(shardSpec.isInChunk(
        createRow("India", "Bengaluru")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("Spain", "Valencia")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("United Kingdom", "London")
    ));
  }

  @Test
  public void testIsInChunk_withNullStart()
  {
    setDimensions("d1", "d2");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        null,
        tupleOf("Spain", "Valencia"),
        10,
        null
    );

    // Verify that anything before (Spain, Valencia) is in chunk
    assertTrue(shardSpec.isInChunk(
        createRow(null, null)
    ));
    assertTrue(shardSpec.isInChunk(
        createRow(null, "Kolkata")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("India", null)
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Kolkata")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Japan", "Tokyo")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Spain", "Barcelona")
    ));

    assertFalse(shardSpec.isInChunk(
        createRow("Spain", "Valencia")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("United Kingdom", "London")
    ));
  }

  @Test
  public void testIsInChunk_withNullEnd()
  {
    setDimensions("d1", "d2");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        tupleOf("India", "Delhi"),
        null,
        10,
        null
    );

    // Verify that anything starting from (India, Delhi) is in chunk
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Kolkata")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Japan", "Tokyo")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Spain", null)
    ));

    assertFalse(shardSpec.isInChunk(
        createRow(null, null)
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("India", null)
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("India", "Bengaluru")
    ));
  }

  @Test
  public void testIsInChunk_withFirstDimEqual()
  {
    setDimensions("d1", "d2");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        tupleOf("India", "Bengaluru"),
        tupleOf("India", "Patna"),
        10,
        null
    );

    // Verify that entries starting from (India, Bengaluru) until (India, Patna) are in chunk
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Bengaluru")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Kolkata")
    ));

    assertFalse(shardSpec.isInChunk(
        createRow("India", "Patna")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("India", "Ahmedabad")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("India", "Raipur")
    ));
  }

  @Test
  public void testIsInChunk_withSingleDimension()
  {
    setDimensions("d1");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        tupleOf("India"),
        tupleOf("Spain"),
        10,
        null
    );

    // Verify that entries starting from (India) until (Spain) are in chunk
    assertTrue(shardSpec.isInChunk(
        createRow("India")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Japan")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Malaysia")
    ));

    assertFalse(shardSpec.isInChunk(
        createRow("Belgium")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("Spain")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("United Kingdom")
    ));
  }

  @Test
  public void testIsInChunk_withMultiValues()
  {
    setDimensions("d1", "d2");

    final MultiDimensionShardSpec shardSpec = new MultiDimensionShardSpec(
        dimensions,
        tupleOf("India", "Delhi"),
        tupleOf("Spain", "Valencia"),
        10,
        null
    );

    // Verify that entries starting from (India, Delhi) until (Spain, Valencia) are in chunk
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Delhi")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("India", "Kolkata")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Japan", "Tokyo")
    ));
    assertTrue(shardSpec.isInChunk(
        createRow("Spain", "Barcelona")
    ));

    assertFalse(shardSpec.isInChunk(
        createRow("India", "Bengaluru")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("Spain", "Valencia")
    ));
    assertFalse(shardSpec.isInChunk(
        createRow("United Kingdom", "London")
    ));
  }

  private void setDimensions(String... dimensionNames)
  {
    dimensions.clear();
    dimensions.addAll(Arrays.asList(dimensionNames));
  }

  private InputRow createRow(String... values)
  {
    Map<String, Object> valueMap = new HashMap<>();
    for (int i = 0; i < dimensions.size(); ++i) {
      valueMap.put(dimensions.get(i), values[i]);
    }
    return new MapBasedInputRow(DateTimes.nowUtc(), dimensions, valueMap);
  }
}
