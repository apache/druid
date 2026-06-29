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

package org.apache.druid.segment;

import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class PhysicalSegmentInspectorTest extends InitializedNullHandlingTest
{
  @Test
  public void testQueryableIndexSegmentProvidesDeprecatedInspector()
  {
    final QueryableIndex index = TestIndex.getMMappedTestIndex();
    // not closed on purpose: the index is a shared static fixture
    final QueryableIndexSegment segment = new QueryableIndexSegment(index, SegmentId.dummy("test"));

    final PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
    Assertions.assertNotNull(inspector);
    Assertions.assertEquals(index.getNumRows(), inspector.getNumRows());
    Assertions.assertEquals(index.getMetadata(), inspector.getMetadata());

    // the deprecated inspector is the union of the interfaces that supersede it
    Assertions.assertNotNull(segment.as(RowCountInspector.class));
    Assertions.assertNotNull(segment.as(PhysicalSegmentColumnInspector.class));
  }

  @Test
  public void testIncrementalIndexSegmentProvidesDeprecatedInspector()
  {
    final IncrementalIndex index = TestIndex.getIncrementalTestIndex();
    // not closed on purpose: the index is a shared static fixture
    final IncrementalIndexSegment segment = new IncrementalIndexSegment(index, SegmentId.dummy("test"));

    final PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
    Assertions.assertNotNull(inspector);
    Assertions.assertEquals(index.numRows(), inspector.getNumRows());
    Assertions.assertEquals(index.getMetadata(), inspector.getMetadata());

    Assertions.assertNotNull(segment.as(RowCountInspector.class));
    Assertions.assertNotNull(segment.as(PhysicalSegmentColumnInspector.class));
  }
}
