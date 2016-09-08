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

package io.druid.segment;

import com.google.common.base.Throwables;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class ReferenceCountingSegmentTest
{
  private ReferenceCountingSegment segment;
  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    segment = new ReferenceCountingSegment(
        new AbstractSegment()
        {
          @Override
          public String getIdentifier()
          {
            return "test_segment";
          }

          @Override
          public Interval getDataInterval()
          {
            return new Interval(DateTime.now().minus(Days.days(1)), DateTime.now());
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
          public void close() throws IOException
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
    final Closeable closeable = segment.increment();
    Assert.assertTrue(segment.getNumReferences() == 1);

    closeable.close();
    closeable.close();
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              closeable.close();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
    Assert.assertTrue(segment.getNumReferences() == 0);
    Assert.assertFalse(segment.isClosed());

    segment.close();
    segment.close();
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              segment.close();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    Assert.assertTrue(segment.getNumReferences() == 0);
    Assert.assertTrue(segment.isClosed());

    segment.increment();
    segment.increment();
    segment.increment();
    Assert.assertTrue(segment.getNumReferences() == 0);

    segment.close();
    Assert.assertTrue(segment.getNumReferences() == 0);
  }
}
