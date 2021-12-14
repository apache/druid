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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DimensionRangeShardSpecTest
{

  private final List<String> dimensions = new ArrayList<>();

  @Test
  public void testIsInChunk()
  {
    setDimensions("d1", "d2");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        StringTuple.create("India", "Delhi"),
        StringTuple.create("Spain", "Valencia"),
        10,
        null
    );

    // Verify that entries starting from (India, Delhi) until (Spain, Valencia) are in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", "Delhi")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", "Kolkata")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Japan", "Tokyo")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Spain", "Barcelona")
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow("India", "Bengaluru")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("Spain", "Valencia")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("United Kingdom", "London")
    ));
  }

  @Test
  public void testIsInChunk_withNullStart()
  {
    setDimensions("d1", "d2");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        null,
        StringTuple.create("Spain", "Valencia"),
        10,
        null
    );

    // Verify that anything before (Spain, Valencia) is in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow(null, null)
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow(null, "Lyon")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", null)
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", "Kolkata")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Japan", "Tokyo")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Spain", "Barcelona")
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow("Spain", "Valencia")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("United Kingdom", "London")
    ));
  }

  @Test
  public void testIsInChunk_withNullEnd()
  {
    setDimensions("d1", "d2");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        StringTuple.create("France", "Lyon"),
        null,
        10,
        null
    );

    // Verify that anything starting from (France, Lyon) is in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow("France", "Paris")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Japan", "Tokyo")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Spain", null)
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow(null, null)
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("France", null)
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("France", "Bordeaux")
    ));
  }

  @Test
  public void testIsInChunk_withFirstDimEqual()
  {
    setDimensions("d1", "d2");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        StringTuple.create("France", "Bordeaux"),
        StringTuple.create("France", "Paris"),
        10,
        null
    );

    // Verify that entries starting from (India, Bengaluru) until (India, Patna) are in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow("France", "Bordeaux")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("France", "Lyon")
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow("France", "Paris")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("France", "Avignon")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("France", "Toulouse")
    ));
  }

  @Test
  public void testIsInChunk_withSingleDimension()
  {
    setDimensions("d1");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        StringTuple.create("India"),
        StringTuple.create("Spain"),
        10,
        null
    );

    // Verify that entries starting from (India) until (Spain) are in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow("India")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Japan")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Malaysia")
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow("Belgium")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("Spain")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("United Kingdom")
    ));
  }

  @Test
  public void testIsInChunk_withMultiValues()
  {
    setDimensions("d1", "d2");

    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        dimensions,
        StringTuple.create("India", "Delhi"),
        StringTuple.create("Spain", "Valencia"),
        10,
        null
    );

    // Verify that entries starting from (India, Delhi) until (Spain, Valencia) are in chunk
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", "Delhi")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("India", "Kolkata")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Japan", "Tokyo")
    ));
    assertTrue(isInChunk(
        shardSpec,
        createRow("Spain", "Barcelona")
    ));

    assertFalse(isInChunk(
        shardSpec,
        createRow("India", "Bengaluru")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("Spain", "Valencia")
    ));
    assertFalse(isInChunk(
        shardSpec,
        createRow("United Kingdom", "London")
    ));
  }

  @Test
  public void testPossibleInDomain_withNullValues()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "India", "Delhi");
    final StringTuple end = StringTuple.create("Krypton", null, "Kryptonopolis"); // null is practically INF here

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // (-INF, INF) * (-INF, INF) * (-INF, INF)
    // 3 -> Accept
    populateDomain(domain,
                   universalSet,
                   universalSet,
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * (-INF, INF) * (-INF, INF)
    // 1.b, 3 -> Accept
    populateDomain(domain,
                   getUnion(Range.singleton("Earth")),
                   universalSet,
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, Earth) U (Krypton, INF) * (-INF, INF) * (-INF, INF)
    // 2 -> Prune
    populateDomain(domain,
                   getUnion(Range.lessThan("Earth"), Range.greaterThan("Krypton")),
                   universalSet,
                   universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, France) * (-INF, INF)
    // 3 -> Accept
    populateDomain(domain,
                   universalSet,
                   getUnion(Range.lessThan("France")),
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Krypton} * (-INF, France] * {Paris}
    // 1.b, 3 -> Accept
    populateDomain(domain,
                   getUnion(Range.singleton("Krypton")),
                   getUnion(Range.atMost("France")),
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));
  }

  @Test
  public void testPossibleInDomain()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "France", "Paris");
    final StringTuple end = StringTuple.create("Earth", "USA", "New York");

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // (-INF, INF) * (-INF, INF) * (-INF, INF)
    // 1.b, 3 -> Accept
    populateDomain(domain,
                   universalSet,
                   universalSet,
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * (-INF, INF) * (-INF, INF)
    // 1.b, 3 -> Accept
    populateDomain(domain,
                   getUnion(Range.singleton("Earth")),
                   universalSet,
                   universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, Earth) U (Earth, INF) * (-INF, INF) * (-INF, INF)
    // 2 -> Prune
    populateDomain(domain,
                   getUnion(Range.lessThan("Earth"), Range.greaterThan("Earth")),
                   universalSet,
                   universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France") * (-INF, INF)
    // 1.b, 2 -> Prune
    populateDomain(domain,
                   universalSet,
                   getUnion(Range.lessThan("France")),
                   universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * [USA, INF) * {New York}
    // 1.b, 1.b, 1.a -> Accept
    populateDomain(domain,
                   universalSet,
                   getUnion(Range.atLeast("USA")),
                   getUnion(Range.singleton("New York"))
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France"] * (Paris, INF)
    // 1.b, 1.b, 3 -> Accept
    populateDomain(domain,
                   universalSet,
                   getUnion(Range.atMost("France")),
                   getUnion(Range.greaterThan("Paris"))
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France] * (-INF, Paris)
    // 1.b, 1.b, 2 -> Prune
    populateDomain(domain,
                   universalSet,
                   getUnion(Range.atMost("France")),
                   getUnion(Range.lessThan("Paris"))
    );
    assertFalse(shard.possibleInDomain(domain));

    // {Earth} * {USA} * {New York}
    // 1.b, 1.b, 1.a -> Accept
    populateDomain(domain,
                   getUnion(Range.singleton("Earth")),
                   getUnion(Range.singleton("USA")),
                   getUnion(Range.singleton("New York"))
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * {USA} * (New York, INF)
    // 1.b, 1.b, 2 -> Prune
    populateDomain(domain,
                   getUnion(Range.singleton("Earth")),
                   getUnion(Range.singleton("USA")),
                   getUnion(Range.greaterThan("New York"))
    );
    assertFalse(shard.possibleInDomain(domain));

    // {Earth} * {India} * Any Non-empty set
    // 1.b, 3 -> Accept
    populateDomain(domain,
                   getUnion(Range.singleton("Earth")),
                   getUnion(Range.singleton("India")),
                   getUnion(Range.greaterThan("New York"))
    );
    assertTrue(shard.possibleInDomain(domain));
  }

  private RangeSet<String> getUnion(Range<String>... ranges)
  {
    RangeSet<String> unionSet = TreeRangeSet.create();
    for (Range<String> range : ranges) {
      unionSet.add(range);
    }
    return unionSet;
  }

  private void populateDomain(Map<String, RangeSet<String>> domain,
                              RangeSet<String> planetSet,
                              RangeSet<String> countrySet,
                              RangeSet<String> citySet)
  {
    domain.clear();
    domain.put("planet", planetSet);
    domain.put("country", countrySet);
    domain.put("city", citySet);
  }

  /**
   * Checks if the given InputRow is in the chunk represented by the given shard spec.
   */
  private boolean isInChunk(DimensionRangeShardSpec shardSpec, InputRow row)
  {
    return DimensionRangeShardSpec.isInChunk(
        shardSpec.getDimensions(),
        shardSpec.getStartTuple(),
        shardSpec.getEndTuple(),
        row
    );
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
