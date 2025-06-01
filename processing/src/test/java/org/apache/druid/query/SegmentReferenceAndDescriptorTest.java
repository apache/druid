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

package org.apache.druid.query;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class SegmentReferenceAndDescriptorTest extends InitializedNullHandlingTest
{
  @Test
  public void testAcquireReference() throws IOException
  {
    final QueryableIndexSegment segment = new QueryableIndexSegment(
        TestIndex.getMMappedTestIndex(), SegmentId.dummy("test")
    );

    final ReferenceCountedSegmentProvider ref = ReferenceCountedSegmentProvider.wrapRootGenerationSegment(segment);
    final SegmentDescriptor descriptor = new SegmentDescriptor(
        segment.getDataInterval(),
        segment.getId().getVersion(),
        segment.getId().getPartitionNum()
    );
    Closer closer = Closer.create();
    final SegmentReferenceAndDescriptor refAndDescriptor = SegmentReferenceAndDescriptor.acquireReference(
        descriptor,
        ref,
        SegmentMapFunction.IDENTITY,
        closer
    );
    Assertions.assertEquals(descriptor, refAndDescriptor.getDescriptor());
    Assertions.assertNotNull(refAndDescriptor.getReference());
    closer.close();
  }
}
