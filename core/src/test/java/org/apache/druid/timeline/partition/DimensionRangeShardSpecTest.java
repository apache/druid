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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
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
  public void testShardSpecLookup()
  {
    setDimensions("dim1", "dim2");

    final List<ShardSpec> shardSpecs = ImmutableList.of(
        new DimensionRangeShardSpec(dimensions, null, StringTuple.create("India", "Delhi"), 1, 1),
        new DimensionRangeShardSpec(
            dimensions,
            StringTuple.create("India", "Delhi"),
            StringTuple.create("Spain", "Valencia"),
            2,
            1
        ),
        new DimensionRangeShardSpec(dimensions, StringTuple.create("Spain", "Valencia"), null, 3, 1)
    );
    final ShardSpecLookup lookup = shardSpecs.get(0).getLookup(shardSpecs);
    final long currentTime = DateTimes.nowUtc().getMillis();

    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            createRow("France", "Paris")
        )
    );

    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            createRow("India", null)
        )
    );

    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            createRow(null, null)
        )
    );

    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            createRow("India", "Delhi")
        )
    );

    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            createRow("India", "Kolkata")
        )
    );

    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            createRow("Spain", null)
        )
    );

    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            createRow("Spain", "Valencia")
        )
    );

    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            createRow("United Kingdom", "London")
        )
    );

    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            createRow("United Kingdom", null)
        )
    );
  }

  @Test
  public void testPossibleInDomain_withNullStart()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = null; // considered to be (-INF, -INF, -INF)
    final StringTuple end = StringTuple.create("Saturn", "Foo", "Bar");

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // {Mars} * {Zoo, Zuu} * {Blah, Random}
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Mars")),
        // EffectiveDomain[:1].size > 1 -> ACCEPT
        getUnion(
            getRangeSet(Range.singleton("Zoo")),
            getRangeSet(Range.singleton("Zuu"))
        ),
        getUnion(
            getRangeSet(Range.singleton("Blah")),
            getRangeSet(Range.singleton("Random"))
        )
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Saturn} * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Saturn")),
        // EffectiveDomain[:1] == {end[:1]}
        universalSet,
        // EffectiveDomain[:2].size > 1 -> ACCEPT
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Saturn} * {Zoo} * (-INF, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Saturn")),
        // EffectiveDomain[:1] == {end[:1]}
        getRangeSet(Range.singleton("Zoo")),
        // EffectiveDomain[:2] == {} -> PRUNE
        universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (Xeon) * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Xeon")),
        // EffectiveDomain[:1] == {} -> PRUNE
        universalSet,
        universalSet
    );
    assertFalse(shard.possibleInDomain(domain));
  }

  @Test
  public void testPossibleInDomain_withNullValues()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "India", "Delhi");
    final StringTuple end = StringTuple.create("Krypton", null, "Kryptonopolis"); // null in end translates to INF

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // (-INF, INF) * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1].size > 1 -> ACCEPT
        universalSet,
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Earth")),
        // EffectiveDomain[:1] == {start[:1]}
        universalSet,
        // EffectiveDomain[:2].size > 1 -> ACCEPT
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, Earth) U (Krypton, INF) * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getUnion(
            getRangeSet(Range.lessThan("Earth")),
            getRangeSet(Range.greaterThan("Krypton"))
        ),
        // EffectiveDomain[:1] = {} -> PRUNE
        universalSet,
        universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, France) * (-INF, INF)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1].size > 2 -> ACCEPT
        getRangeSet(Range.lessThan("France")),
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Jupiter} * (Foo) * {Bar}
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Jupiter")),
        // EffectiveDomain[:1] != {} OR {start[:1]} OR {end[:1]} -> ACCEPT
        getRangeSet(Range.singleton("Foo")),
        getRangeSet(Range.singleton("Bar"))
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Krypton} * (-INF, France] * {Paris}
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Krypton")),
        // EffectiveDomain[:1] == {end[:1]}
        getRangeSet(Range.atMost("France")),
        // EffectiveDomain[:2].size > 1 -> ACCEPT
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));
  }

  @Test
  public void testPossibleInDomain_nonNullValues_acceptanceScenarios()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "France", "Paris");
    final StringTuple end = StringTuple.create("Earth", "USA", "New York");

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // (-INF, INF) * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        universalSet,
        // EffectiveDomain[:2].size > 1 -> ACCEPT
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Earth")),
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        universalSet,
        // EffectiveDomain[:2].size > 1 -> ACCEPT
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, INF) * [USA, INF) * {New York}
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.atLeast("USA")),
        // EffectiveDomain[:2] == {end[:2]}
        getRangeSet(Range.singleton("New York"))
        // EffectiveDomain[:3] == {end[:3]}
    );
    //EffectiveDomain[:].size > 0 -> ACCEPT
    assertTrue(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France"] * (Paris, INF)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.atMost("France")),
        // EffectiveDomain[:2] == {<Earth, France>} == {start[:2]}
        getRangeSet(Range.greaterThan("Paris"))
        // EffectiveDomain[:3].size > 1 -> ACCEPT
    );
    assertTrue(shard.possibleInDomain(domain));

    // {Earth} * {India} * Any Non-empty set
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Earth")),
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.singleton("India")),
        // EffectiveDomain[:2] == {<Earth, India>} != {start[:2]} OR {end[:2]}
        getRangeSet(Range.greaterThan("New York"))
        // EffectiveDomain[:3].size > 1 -> ACCEPT
    );
    assertTrue(shard.possibleInDomain(domain));
  }

  @Test
  public void testPossibleInDomain_nonNullValues_pruningScenarios()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "France", "Paris");
    final StringTuple end = StringTuple.create("Earth", "USA", "New York");

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // (-INF, Earth) U (Earth, INF) * (-INF, INF) * (-INF, INF)
    populateDomain(
        domain,
        getUnion(
            getRangeSet(Range.lessThan("Earth")),
            getRangeSet(Range.greaterThan("Earth"))
        ),
        // EffectiveDomain[:1] == {} -> PRUNE
        universalSet,
        universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France") * (-INF, INF)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.lessThan("France")),
        // EffectiveDomain[:2] == {} -> PRUNE
        universalSet
    );
    assertFalse(shard.possibleInDomain(domain));

    // (-INF, INF) * (-INF, "France] * (-INF, Paris)
    populateDomain(
        domain,
        universalSet,
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.atMost("France")),
        // EffectiveDomain[:2] == {<Earth, France>} == {start[:2]}
        getRangeSet(Range.lessThan("Paris"))
        // EffectiveDomain[:3] == {} -> PRUNE
    );
    assertFalse(shard.possibleInDomain(domain));

    // {Earth} * {USA} * (New York, INF)
    populateDomain(
        domain,
        getRangeSet(Range.singleton("Earth")),
        // EffectiveDomain[:1] == {Earth} == {start[:1]} == {end[:1]}
        getRangeSet(Range.singleton("USA")),
        // EffectiveDomain[:2] == {<Earth, USA>} == {end[:2]}
        getRangeSet(Range.greaterThan("New York"))
        // EffectiveDomain[:3] == {} -> PRUNE
    );
    assertFalse(shard.possibleInDomain(domain));
  }
  @Test
  public void testPossibleInDomain_falsePruning()
  {
    setDimensions("planet", "country", "city");

    final StringTuple start = StringTuple.create("Earth", "France", "Paris");
    final StringTuple end = StringTuple.create("Mars", "USA", "New York");

    final RangeSet<String> universalSet = TreeRangeSet.create();
    universalSet.add(Range.all());

    ShardSpec shard = new DimensionRangeShardSpec(dimensions, start, end, 0, null);
    Map<String, RangeSet<String>> domain = new HashMap<>();

    // {Earth} U {Mars} * (USA, INF) * (-INF, INF)
    populateDomain(
        domain,
        getUnion(
            getRangeSet(Range.singleton("Earth")),
            getRangeSet(Range.singleton("Mars"))
        ),
        getUnion(
            getRangeSet(Range.greaterThan("USA"))
        ),
        universalSet
    );
    assertTrue(shard.possibleInDomain(domain));
  }

  private RangeSet<String> getRangeSet(Range range)
  {
    RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(range);
    return rangeSet;
  }

  private RangeSet<String> getUnion(RangeSet<String>... rangeSets)
  {
    RangeSet<String> unionSet = TreeRangeSet.create();
    for (RangeSet<String> range : rangeSets) {
      unionSet.addAll(range);
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
