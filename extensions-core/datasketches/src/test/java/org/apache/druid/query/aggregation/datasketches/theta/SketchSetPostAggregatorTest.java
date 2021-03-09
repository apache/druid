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
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SketchSetPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConstructorNumFields()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Illegal number of fields[0], must be > 1");
    new SketchSetPostAggregator(
        "summary",
        "UNION",
        null,
        ImmutableList.of()
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    List<PostAggregator> fields = Arrays.asList(
        new FieldAccessPostAggregator("field1", "sketch"),
        new FieldAccessPostAggregator("field2", "sketch")
    );
    final PostAggregator union = new SketchSetPostAggregator(
        "summary",
        "UNION",
        null,
        fields
    );
    final PostAggregator intersect = new SketchSetPostAggregator(
        "summary",
        "INTERSECT",
        null,
        fields
    );
    final PostAggregator not = new SketchSetPostAggregator(
        "summary",
        "NOT",
        null,
        fields
    );
    List<PostAggregator> serdeTests = Arrays.asList(union, intersect, not);

    for (PostAggregator there : serdeTests) {
      DefaultObjectMapper mapper = new DefaultObjectMapper();
      SketchSetPostAggregator andBackAgain = mapper.readValue(
          mapper.writeValueAsString(there),
          SketchSetPostAggregator.class
      );

      Assert.assertEquals(there, andBackAgain);
      Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    }
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new SketchSetPostAggregator(
        "summary",
        "UNION",
        null,
        Arrays.asList(
            new FieldAccessPostAggregator("field1", "sketch"),
            new FieldAccessPostAggregator("field2", "sketch")
        )
    );

    Assert.assertEquals(
        "SketchSetPostAggregator{name='summary', fields=[FieldAccessPostAggregator{name='field1', fieldName='sketch'}, FieldAccessPostAggregator{name='field2', fieldName='sketch'}], func=UNION, size=16384}",
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
    Union u3 = (Union) SetOperation.builder().setNominalEntries(10).build(Family.UNION);
    u3.update(10L);
    Union u4 = (Union) SetOperation.builder().setNominalEntries(10).build(Family.UNION);
    u4.update(20L);

    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u1)).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u2)).anyTimes();
    PostAggregator field3 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field3.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u3)).anyTimes();
    PostAggregator field4 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field4.compute(EasyMock.anyObject(Map.class))).andReturn(SketchHolder.of(u4)).anyTimes();
    EasyMock.replay(field1, field2, field3, field4);

    SketchSetPostAggregator postAgg1 = new SketchSetPostAggregator(
        "summary",
        "UNION",
        null,
        Arrays.asList(field1, field2)
    );
    SketchSetPostAggregator postAgg2 = new SketchSetPostAggregator(
        "summary",
        "UNION",
        null,
        Arrays.asList(field3, field4)
    );
    SketchHolder holder1 = (SketchHolder) postAgg1.compute(ImmutableMap.of());
    SketchHolder holder2 = (SketchHolder) postAgg2.compute(ImmutableMap.of());
    Assert.assertEquals(0, postAgg1.getComparator().compare(holder1, holder2));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(SketchSetPostAggregator.class)
                  .withNonnullFields("name", "fields")
                  .usingGetClass()
                  .verify();
  }
}
