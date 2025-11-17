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

package org.apache.druid.segment.realtime;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class FireHydrantTest extends InitializedNullHandlingTest
{
  private IncrementalIndexSegment incrementalIndexSegment;
  private QueryableIndexSegment queryableIndexSegment;
  private FireHydrant hydrant;

  @Before
  public void setup()
  {
    incrementalIndexSegment = new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SegmentId.dummy("test"));
    queryableIndexSegment = new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test"));

    // hydrant starts out with incremental segment loaded
    hydrant = new FireHydrant(incrementalIndexSegment, 0);
  }

  @Test
  public void testAcquireSegmentNotSwapped()
  {
    Assert.assertEquals(0, hydrant.getHydrantSegment().getNumReferences());
    Segment segment = hydrant.acquireSegment();
    Assert.assertNotNull(segment);
    Assert.assertSame(incrementalIndexSegment, ((ReferenceCountedSegmentProvider.ReferenceClosingSegment) segment).getProvider().getBaseSegment());
    Assert.assertEquals(1, hydrant.getHydrantSegment().getNumReferences());
  }

  @Test
  public void testAcquireSegmentSwapped()
  {
    ReferenceCountedSegmentProvider incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    hydrant.swapSegment(queryableIndexSegment);
    Segment segment = hydrant.acquireSegment();
    Assert.assertNotNull(segment);
    Assert.assertSame(queryableIndexSegment, ((ReferenceCountedSegmentProvider.ReferenceClosingSegment) segment).getProvider().getBaseSegment());
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(1, hydrant.getHydrantSegment().getNumReferences());
  }

  @Test
  public void testAcquireSegmentClosed()
  {
    hydrant.getHydrantSegment().close();
    Assert.assertEquals(0, hydrant.getHydrantSegment().getNumReferences());
    Throwable t = Assert.assertThrows(ISE.class, hydrant::acquireSegment);
    Assert.assertEquals("segment.close() is called somewhere outside FireHydrant.swapSegment()", t.getMessage());
  }

  @Test
  public void testGetSegmentForQuery() throws IOException
  {
    ReferenceCountedSegmentProvider incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());

    Optional<Segment> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        SegmentMapFunction.IDENTITY
    );
    Assert.assertTrue(maybeSegmentAndCloseable.isPresent());
    Assert.assertEquals(1, incrementalSegmentReference.getNumReferences());

    Segment segmentAndCloseable = maybeSegmentAndCloseable.get();
    segmentAndCloseable.close();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
  }

  @Test
  public void testGetSegmentForQuerySwapped() throws IOException
  {
    ReferenceCountedSegmentProvider incrementalSegmentReference = hydrant.getHydrantSegment();
    hydrant.swapSegment(queryableIndexSegment);
    ReferenceCountedSegmentProvider queryableSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(0, queryableSegmentReference.getNumReferences());

    Optional<Segment> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        SegmentMapFunction.IDENTITY
    );
    Assert.assertTrue(maybeSegmentAndCloseable.isPresent());
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(1, queryableSegmentReference.getNumReferences());

    Segment segmentAndCloseable = maybeSegmentAndCloseable.get();
    segmentAndCloseable.close();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(0, queryableSegmentReference.getNumReferences());
  }

  @Test
  public void testGetSegmentForQuerySwappedWithNull()
  {
    ReferenceCountedSegmentProvider incrementalSegmentReference = hydrant.getHydrantSegment();
    hydrant.swapSegment(null);
    ReferenceCountedSegmentProvider queryableSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertNull(queryableSegmentReference);

    Optional<Segment> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        SegmentMapFunction.IDENTITY
    );
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertFalse(maybeSegmentAndCloseable.isPresent());
  }

  @Test
  public void testGetSegmentForQueryButNotAbleToAcquireReferencesSegmentClosed()
  {
    ReferenceCountedSegmentProvider incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    incrementalSegmentReference.close();

    Throwable t = Assert.assertThrows(
        ISE.class,
        () -> hydrant.getSegmentForQuery(SegmentMapFunction.IDENTITY)
    );
    Assert.assertEquals("segment.close() is called somewhere outside FireHydrant.swapSegment()", t.getMessage());
  }

  @Test
  @SuppressWarnings("ReturnValueIgnored")
  public void testToStringWhenSwappedWithNull()
  {
    hydrant.swapSegment(null);
    hydrant.toString();
  }
}
