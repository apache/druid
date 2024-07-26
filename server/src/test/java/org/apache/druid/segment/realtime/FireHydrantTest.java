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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

public class FireHydrantTest extends InitializedNullHandlingTest
{
  private IncrementalIndexSegment incrementalIndexSegment;
  private QueryableIndexSegment queryableIndexSegment;
  private FireHydrant hydrant;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    incrementalIndexSegment = new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SegmentId.dummy("test"));
    queryableIndexSegment = new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test"));

    // hydrant starts out with incremental segment loaded
    hydrant = new FireHydrant(incrementalIndexSegment, 0);
  }

  @Test
  public void testGetIncrementedSegmentNotSwapped()
  {
    Assert.assertEquals(0, hydrant.getHydrantSegment().getNumReferences());
    ReferenceCountingSegment segment = hydrant.getIncrementedSegment();
    Assert.assertNotNull(segment);
    Assert.assertTrue(segment.getBaseSegment() == incrementalIndexSegment);
    Assert.assertEquals(1, segment.getNumReferences());
  }

  @Test
  public void testGetIncrementedSegmentSwapped()
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    hydrant.swapSegment(queryableIndexSegment);
    ReferenceCountingSegment segment = hydrant.getIncrementedSegment();
    Assert.assertNotNull(segment);
    Assert.assertTrue(segment.getBaseSegment() == queryableIndexSegment);
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(1, segment.getNumReferences());
  }

  @Test
  public void testGetIncrementedSegmentClosed()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("segment.close() is called somewhere outside FireHydrant.swapSegment()");
    hydrant.getHydrantSegment().close();
    Assert.assertEquals(0, hydrant.getHydrantSegment().getNumReferences());
    ReferenceCountingSegment segment = hydrant.getIncrementedSegment();
  }

  @Test
  public void testGetAndIncrementSegment() throws IOException
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());

    Pair<ReferenceCountingSegment, Closeable> segmentAndCloseable = hydrant.getAndIncrementSegment();
    Assert.assertEquals(1, segmentAndCloseable.lhs.getNumReferences());
    segmentAndCloseable.rhs.close();
    Assert.assertEquals(0, segmentAndCloseable.lhs.getNumReferences());
  }

  @Test
  public void testGetSegmentForQuery() throws IOException
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());

    Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        Function.identity()
    );
    Assert.assertTrue(maybeSegmentAndCloseable.isPresent());
    Assert.assertEquals(1, incrementalSegmentReference.getNumReferences());

    Pair<SegmentReference, Closeable> segmentAndCloseable = maybeSegmentAndCloseable.get();
    segmentAndCloseable.rhs.close();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
  }

  @Test
  public void testGetSegmentForQuerySwapped() throws IOException
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    hydrant.swapSegment(queryableIndexSegment);
    ReferenceCountingSegment queryableSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(0, queryableSegmentReference.getNumReferences());

    Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        Function.identity()
    );
    Assert.assertTrue(maybeSegmentAndCloseable.isPresent());
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(1, queryableSegmentReference.getNumReferences());

    Pair<SegmentReference, Closeable> segmentAndCloseable = maybeSegmentAndCloseable.get();
    segmentAndCloseable.rhs.close();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertEquals(0, queryableSegmentReference.getNumReferences());
  }

  @Test
  public void testGetSegmentForQuerySwappedWithNull()
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    hydrant.swapSegment(null);
    ReferenceCountingSegment queryableSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertNull(queryableSegmentReference);

    Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        Function.identity()
    );
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    Assert.assertFalse(maybeSegmentAndCloseable.isPresent());
  }

  @Test
  public void testGetSegmentForQueryButNotAbleToAcquireReferences()
  {
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());

    Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        segmentReference -> new SegmentReference()
        {
          @Override
          public Optional<Closeable> acquireReferences()
          {
            return Optional.empty();
          }

          @Override
          public SegmentId getId()
          {
            return incrementalIndexSegment.getId();
          }

          @Override
          public Interval getDataInterval()
          {
            return incrementalIndexSegment.getDataInterval();
          }

          @Nullable
          @Override
          public QueryableIndex asQueryableIndex()
          {
            return incrementalIndexSegment.asQueryableIndex();
          }

          @Override
          public StorageAdapter asStorageAdapter()
          {
            return incrementalIndexSegment.asStorageAdapter();
          }

          @Override
          public void close()
          {
            incrementalIndexSegment.close();
          }
        }
    );
    Assert.assertFalse(maybeSegmentAndCloseable.isPresent());
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
  }

  @Test
  public void testGetSegmentForQueryButNotAbleToAcquireReferencesSegmentClosed()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("segment.close() is called somewhere outside FireHydrant.swapSegment()");
    ReferenceCountingSegment incrementalSegmentReference = hydrant.getHydrantSegment();
    Assert.assertEquals(0, incrementalSegmentReference.getNumReferences());
    incrementalSegmentReference.close();

    Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable = hydrant.getSegmentForQuery(
        Function.identity()
    );
  }

  @Test
  @SuppressWarnings("ReturnValueIgnored")
  public void testToStringWhenSwappedWithNull()
  {
    hydrant.swapSegment(null);
    hydrant.toString();
  }
}
