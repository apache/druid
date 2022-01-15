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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SegmentLocalCacheLoaderTest
{

  SegmentLocalCacheLoader segmentLocalCacheLoader;

  @Before
  public void setUp()
  {
    segmentLocalCacheLoader = new SegmentLocalCacheLoader(null, null, null);
  }

  @Test
  public void testGetSegmentWithTombstones() throws SegmentLoadingException
  {
    Interval interval = Intervals.of("2014-01-01/2014-01-02");
    DataSegment tombstone = new DataSegment("foo", interval, "version",
                                            ImmutableMap.of("type", "tombstone"),
                                            null, null, null, null, 0
    );


    ReferenceCountingSegment segment = segmentLocalCacheLoader.getSegment(tombstone, false, null);

    Assert.assertNotNull(segment.getId());
    Assert.assertEquals(interval, segment.getDataInterval());
    Assert.assertNotNull(segment.asStorageAdapter());

    Assert.assertTrue(segment.asQueryableIndex().isFromTombstone());

    Assert.assertEquals(interval, segment.asQueryableIndex().getDataInterval());
    Assert.assertThrows(UnsupportedOperationException.class, () -> segment.asQueryableIndex().getMetadata());
    Assert.assertThrows(UnsupportedOperationException.class, () -> segment.asQueryableIndex().getNumRows());
    Assert.assertThrows(UnsupportedOperationException.class, () -> segment.asQueryableIndex().getAvailableDimensions());
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> segment.asQueryableIndex().getBitmapFactoryForDimensions()
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> segment.asQueryableIndex().getDimensionHandlers()
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> segment.asQueryableIndex().getColumnHolder(null)
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> segment.asQueryableIndex().getColumnHolder(null)
    );

  }
}
