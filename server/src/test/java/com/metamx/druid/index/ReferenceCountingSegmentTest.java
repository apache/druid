/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.index;

import com.google.common.base.Throwables;
import com.metamx.druid.StorageAdapter;
import junit.framework.Assert;
import org.joda.time.Interval;
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
        new Segment()
        {
          @Override
          public String getIdentifier()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Interval getDataInterval()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public QueryableIndex asQueryableIndex()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public StorageAdapter asStorageAdapter()
          {
            throw new UnsupportedOperationException();
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
    segment.close();
    Assert.assertTrue(segment.getNumReferences() == 0);
  }
}
