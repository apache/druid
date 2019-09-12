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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class ReferenceCountingSegmentTest
{
  private ReferenceCountingSegment segment;
  private ExecutorService exec;

  @Before
  public void setUp()
  {
    segment = ReferenceCountingSegment.wrapRootGenerationSegment(
        new AbstractSegment()
        {
          @Override
          public SegmentId getId()
          {
            return SegmentId.dummy("test_segment");
          }

          @Override
          public Interval getDataInterval()
          {
            return new Interval(DateTimes.nowUtc().minus(Days.days(1)), DateTimes.nowUtc());
          }

          @Override
          public QueryableIndex asQueryableIndex()
          {
            return null;
          }

          @Override
          public StorageAdapter asStorageAdapter()
          {
            return null;
          }

          @Override
          public void close()
          {
          }
        }
    );

    exec = Executors.newSingleThreadExecutor();
  }

  @Test
  public void testMultipleClose() throws Exception
  {
    Assert.assertFalse(segment.isClosed());
    Assert.assertTrue(segment.increment());
    Assert.assertEquals(1, segment.getNumReferences());

    Closeable closeable = segment.decrementOnceCloseable();
    closeable.close();
    closeable.close();
    exec.submit(
        () -> {
          try {
            closeable.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    ).get();
    Assert.assertEquals(0, segment.getNumReferences());
    Assert.assertFalse(segment.isClosed());

    segment.close();
    segment.close();
    exec.submit(
        () -> {
          try {
            segment.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    ).get();

    Assert.assertEquals(0, segment.getNumReferences());
    Assert.assertTrue(segment.isClosed());

    segment.increment();
    segment.increment();
    segment.increment();
    Assert.assertEquals(0, segment.getNumReferences());

    segment.close();
    Assert.assertEquals(0, segment.getNumReferences());
  }
}
