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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SketchToStringPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new SketchToStringPostAggregator(
        "summary",
        new FieldAccessPostAggregator("field", "sketch")
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    SketchToStringPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        SketchToStringPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new SketchToStringPostAggregator(
        "summary",
        new FieldAccessPostAggregator("field", "sketch")
    );

    Assert.assertEquals(
        "SketchToStringPostAggregator{name='summary', field=FieldAccessPostAggregator{name='field', fieldName='sketch'}}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    Union u1 = (Union) SetOperation.builder().setNominalEntries(10).build(Family.UNION);
    u1.update(10L);
    Union u2 = (Union) SetOperation.builder().setNominalEntries(10).build(Family.UNION);
    u2.update(20L);

    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u1)).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u2)).anyTimes();
    EasyMock.replay(field1, field2);

    SketchToStringPostAggregator postAgg1 = new SketchToStringPostAggregator(
        "summary",
        field1
    );
    SketchToStringPostAggregator postAgg2 = new SketchToStringPostAggregator(
        "summary",
        field2
    );
    String summary1 = (String) postAgg1.compute(ImmutableMap.of());
    String summary2 = (String) postAgg2.compute(ImmutableMap.of());
    Assert.assertEquals(0, postAgg1.getComparator().compare(summary1, summary2));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(SketchToStringPostAggregator.class)
                  .withNonnullFields("name", "field")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testCompute()
  {
    // not going to iterate over the selector since getting a summary of an empty sketch is sufficient
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(new Object[0]);
    final Aggregator agg = new SketchAggregator(selector, 4096);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new SketchToStringPostAggregator(
        "summary",
        new FieldAccessPostAggregator("field", "sketch")
    );

    final String summary = (String) postAgg.compute(fields);
    Assert.assertNotNull(summary);
    Assert.assertTrue(summary.contains("SUMMARY"));
  }

}
