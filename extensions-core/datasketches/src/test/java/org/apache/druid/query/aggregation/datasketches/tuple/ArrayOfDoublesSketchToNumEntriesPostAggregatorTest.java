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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ArrayOfDoublesSketchToNumEntriesPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchToNumEntriesPostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchToNumEntriesPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchToNumEntriesPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }


  @Test
  public void testToString()
  {
    PostAggregator postAgg = new ArrayOfDoublesSketchToNumEntriesPostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchToNumEntriesPostAggregator{name='a', field=ConstantPostAggregator{name='', constantValue=0}}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    ArrayOfDoublesUpdatableSketch s1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();

    s1.update("foo", new double[]{1.0, 2.0});
    ArrayOfDoublesUpdatableSketch s2 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();
    s2.update("foo", new double[]{2.0, 2.0});
    s2.update("bar", new double[]{3.0, 4.0});
    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(s2).anyTimes();
    EasyMock.replay(field1, field2);

    final ArrayOfDoublesSketchToNumEntriesPostAggregator postAgg1 = new ArrayOfDoublesSketchToNumEntriesPostAggregator(
        "a",
        field1
    );
    final ArrayOfDoublesSketchToNumEntriesPostAggregator postAgg2 = new ArrayOfDoublesSketchToNumEntriesPostAggregator(
        "a",
        field2
    );
    // computes number of entries per sketch, which is 1 for s1 and 2 for s2
    Integer numEntries1 = postAgg1.compute(ImmutableMap.of());
    Integer numEntries2 = postAgg2.compute(ImmutableMap.of());
    Assert.assertEquals(-1, postAgg1.getComparator().compare(numEntries1, numEntries2));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchToNumEntriesPostAggregator.class)
                  .withNonnullFields("name", "field")
                  .withIgnoredFields("dependentFields")
                  .usingGetClass()
                  .verify();
  }
}
