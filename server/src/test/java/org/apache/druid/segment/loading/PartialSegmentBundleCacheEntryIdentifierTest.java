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

package org.apache.druid.segment.loading;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PartialSegmentBundleCacheEntryIdentifierTest
{
  @Test
  void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(PartialSegmentBundleCacheEntryIdentifier.class).usingGetClass().verify();
  }

  @Test
  void testNotEqualToSegmentCacheEntryIdentifierWithSameSegmentId()
  {
    final SegmentId segmentId = SegmentId.of("ds", Intervals.of("2025/2026"), "v1", 0);
    final PartialSegmentBundleCacheEntryIdentifier bundle = new PartialSegmentBundleCacheEntryIdentifier(
        segmentId,
        "__base"
    );
    final SegmentCacheEntryIdentifier segment = new SegmentCacheEntryIdentifier(segmentId);
    Assertions.assertNotEquals(bundle, segment);
    Assertions.assertNotEquals(segment, bundle);
  }
}
