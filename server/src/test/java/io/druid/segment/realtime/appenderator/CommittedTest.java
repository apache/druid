/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CommittedTest
{
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();

  private static final SegmentIdentifier IDENTIFIER_OBJECT1 = new SegmentIdentifier(
      "foo",
      new Interval("2000/2001"),
      "2000",
      new LinearShardSpec(1)
  );

  private static final SegmentIdentifier IDENTIFIER_OBJECT2 = new SegmentIdentifier(
      "foo",
      new Interval("2001/2002"),
      "2001",
      new LinearShardSpec(1)
  );

  private static final SegmentIdentifier IDENTIFIER_OBJECT3 = new SegmentIdentifier(
      "foo",
      new Interval("2001/2002"),
      "2001",
      new LinearShardSpec(2)
  );

  private static final String IDENTIFIER1 = IDENTIFIER_OBJECT1.getIdentifierAsString();
  private static final String IDENTIFIER2 = IDENTIFIER_OBJECT2.getIdentifierAsString();
  private static final String IDENTIFIER3 = IDENTIFIER_OBJECT3.getIdentifierAsString();

  private static Committed fixedInstance()
  {
    final Map<String, Integer> hydrants = Maps.newHashMap();
    hydrants.put(IDENTIFIER1, 3);
    hydrants.put(IDENTIFIER2, 2);
    return new Committed(hydrants, ImmutableMap.of("metadata", "foo"));
  }

  @Test
  public void testFactoryMethod()
  {
    final Committed committed = fixedInstance();
    final Committed committed2 = Committed.create(
        ImmutableMap.of(
            IDENTIFIER_OBJECT1, 3,
            IDENTIFIER_OBJECT2, 2
        ),
        ImmutableMap.of("metadata", "foo")
    );
    Assert.assertEquals(committed, committed2);
  }

  @Test
  public void testSerde() throws Exception
  {
    final Committed committed = fixedInstance();
    final byte[] bytes = objectMapper.writeValueAsBytes(committed);
    final Committed committed2 = objectMapper.readValue(bytes, Committed.class);
    Assert.assertEquals("Round trip: overall", committed, committed2);
    Assert.assertEquals("Round trip: metadata", committed.getMetadata(), committed2.getMetadata());
    Assert.assertEquals("Round trip: identifiers", committed.getHydrants().keySet(), committed2.getHydrants().keySet());
  }

  @Test
  public void testGetCommittedHydrant()
  {
    Assert.assertEquals(3, fixedInstance().getCommittedHydrants(IDENTIFIER1));
    Assert.assertEquals(2, fixedInstance().getCommittedHydrants(IDENTIFIER2));
    Assert.assertEquals(0, fixedInstance().getCommittedHydrants(IDENTIFIER3));
  }

  @Test
  public void testWithout() throws Exception
  {
    Assert.assertEquals(0, fixedInstance().without(IDENTIFIER1).getCommittedHydrants(IDENTIFIER1));
    Assert.assertEquals(2, fixedInstance().without(IDENTIFIER1).getCommittedHydrants(IDENTIFIER2));
  }
}
