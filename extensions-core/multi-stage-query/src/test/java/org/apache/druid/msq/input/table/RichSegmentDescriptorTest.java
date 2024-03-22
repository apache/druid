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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class RichSegmentDescriptorTest
{
  @Test
  public void testSerdeWithFullIntervalDifferentFromInterval() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final RichSegmentDescriptor descriptor = new RichSegmentDescriptor(
        Intervals.of("2000/2002"),
        Intervals.of("2000/2001"),
        "2",
        3
    );

    Assert.assertEquals(
        descriptor,
        mapper.readValue(mapper.writeValueAsString(descriptor), RichSegmentDescriptor.class)
    );
  }

  @Test
  public void testSerdeWithFullIntervalSameAsInterval() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final RichSegmentDescriptor descriptor = new RichSegmentDescriptor(
        Intervals.of("2000/2001"),
        Intervals.of("2000/2001"),
        "2",
        3
    );

    Assert.assertEquals(
        descriptor,
        mapper.readValue(mapper.writeValueAsString(descriptor), RichSegmentDescriptor.class)
    );
  }

  @Test
  public void testDeserializeRichSegmentDescriptorAsSegmentDescriptor() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final RichSegmentDescriptor descriptor = new RichSegmentDescriptor(
        Intervals.of("2000/2002"),
        Intervals.of("2000/2001"),
        "2",
        3
    );

    Assert.assertEquals(
        new SegmentDescriptor(Intervals.of("2000/2001"), "2", 3),
        mapper.readValue(mapper.writeValueAsString(descriptor), SegmentDescriptor.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(RichSegmentDescriptor.class).usingGetClass().verify();
  }
}
