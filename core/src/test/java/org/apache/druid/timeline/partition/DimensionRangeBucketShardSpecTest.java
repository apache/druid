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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

public class DimensionRangeBucketShardSpecTest
{

  private static final List<String> DIMENSIONS = Arrays.asList("dim1", "dim2");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConvert()
  {
    Assert.assertEquals(
        new BuildingDimensionRangeShardSpec(
            1,
            DIMENSIONS,
            StringTuple.create("start1", "start2"),
            StringTuple.create("end1", "end2"),
            5
        ),
        new DimensionRangeBucketShardSpec(
            1,
            DIMENSIONS,
            StringTuple.create("start1", "start2"),
            StringTuple.create("end1", "end2")
        ).convert(5)
    );
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(1, 0, "test"),
        new DimensionRangeBucketShardSpec(
            1,
            DIMENSIONS,
            StringTuple.create("start1", "start2"),
            StringTuple.create("end1", "end2")
        ).createChunk("test")
    );
  }

  @Test
  public void testShardSpecLookup()
  {
    final List<ShardSpec> shardSpecs = ImmutableList.of(
        new DimensionRangeBucketShardSpec(0, DIMENSIONS, null, StringTuple.create("c", "12")),
        new DimensionRangeBucketShardSpec(
            1,
            DIMENSIONS,
            StringTuple.create("f", "13"),
            StringTuple.create("i", "9")
        ),
        new DimensionRangeBucketShardSpec(2, DIMENSIONS, StringTuple.create("i", "9"), null)
    );
    final ShardSpecLookup lookup = shardSpecs.get(0).getLookup(shardSpecs);
    final long currentTime = DateTimes.nowUtc().getMillis();
    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                DIMENSIONS,
                ImmutableMap.of(DIMENSIONS.get(0), "a", DIMENSIONS.get(1), "12", "time", currentTime)
            )
        )
    );
    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                DIMENSIONS, ImmutableMap.of(DIMENSIONS.get(0), "g", DIMENSIONS.get(1), "8", "time", currentTime)
            )
        )
    );
    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                DIMENSIONS, ImmutableMap.of(DIMENSIONS.get(0), "k", DIMENSIONS.get(1), "14", "time", currentTime)
            )
        )
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.registerSubtypes(new NamedType(
        DimensionRangeBucketShardSpec.class,
        DimensionRangeBucketShardSpec.TYPE
    ));
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final DimensionRangeBucketShardSpec original = new DimensionRangeBucketShardSpec(
        1,
        DIMENSIONS,
        StringTuple.create("start1", "start2"),
        StringTuple.create("end1", "end2")
    );
    final String json = mapper.writeValueAsString(original);
    final DimensionRangeBucketShardSpec fromJson = (DimensionRangeBucketShardSpec) mapper.readValue(
        json,
        ShardSpec.class
    );
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testInvalidStartTupleSize()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Start tuple must either be null or of the same size as the number of partition dimensions"
    );

    new DimensionRangeBucketShardSpec(
        1,
        DIMENSIONS,
        StringTuple.create("a"),
        null
    );
  }

  @Test
  public void testInvalidEndTupleSize()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "End tuple must either be null or of the same size as the number of partition dimensions"
    );

    new DimensionRangeBucketShardSpec(
        1,
        DIMENSIONS,
        StringTuple.create("a", "b"),
        StringTuple.create("e", "f", "g")
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DimensionRangeBucketShardSpec.class).usingGetClass().verify();
  }
}
