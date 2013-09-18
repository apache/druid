/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment.data;

import com.google.common.io.Closeables;
import com.google.common.primitives.Floats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.Channels;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CompressedFloatsIndexedSupplierTest
{
  private IndexedFloats indexed;
  private CompressedFloatsIndexedSupplier supplier;
  private float[] vals;

  @Before
  public void setUp() throws Exception
  {
    Closeables.close(indexed, false);
    indexed = null;
    supplier = null;
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    Closeables.close(indexed, false);
  }

  private void setupSimple()
  {
    vals = new float[]{
        0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 0.10f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f
    };

    supplier = CompressedFloatsIndexedSupplier.fromFloatBuffer(
        FloatBuffer.wrap(vals),
        5,
        ByteOrder.nativeOrder()
    );

    indexed = supplier.get();
  }

  private void setupSimpleWithSerde() throws IOException
  {
    vals = new float[]{
        0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 0.10f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f
    };

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressedFloatsIndexedSupplier theSupplier = CompressedFloatsIndexedSupplier.fromFloatBuffer(
        FloatBuffer.wrap(vals), 5, ByteOrder.nativeOrder()
    );
    theSupplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedFloatsIndexedSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), ByteOrder.nativeOrder());
    indexed = supplier.get();
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple();

    Assert.assertEquals(4, supplier.getBaseFloatBuffers().size());

    Assert.assertEquals(vals.length, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i), 0.0);
    }
  }

  @Test
  public void testBulkFill() throws Exception
  {
    setupSimple();

    tryFill(0, 15);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuch() throws Exception
  {
    setupSimple();
    tryFill(7, 10);
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde();

    Assert.assertEquals(4, supplier.getBaseFloatBuffers().size());

    Assert.assertEquals(vals.length, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i), 0.0);
    }
  }

  @Test
  public void testBulkFillWithSerde() throws Exception
  {
    setupSimpleWithSerde();

    tryFill(0, 15);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuchWithSerde() throws Exception
  {
    setupSimpleWithSerde();
    tryFill(7, 10);
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  @Test
  public void testConcurrentThreadReads() throws Exception
  {
    setupSimple();

    final AtomicReference<String> reason = new AtomicReference<String>("none");

    final int numRuns = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(2);
    final AtomicBoolean failureHappened = new AtomicBoolean(false);
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startLatch.await();
        }
        catch (InterruptedException e) {
          failureHappened.set(true);
          reason.set("interrupt.");
          stopLatch.countDown();
          return;
        }

        try {
          for (int i = 0; i < numRuns; ++i) {
            for (int j = 0; j < indexed.size(); ++j) {
              final float val = vals[j];
              final float indexedVal = indexed.get(j);
              if (Floats.compare(val, indexedVal) != 0) {
                failureHappened.set(true);
                reason.set(String.format("Thread1[%d]: %f != %f", j, val, indexedVal));
                stopLatch.countDown();
                return;
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          failureHappened.set(true);
          reason.set(e.getMessage());
        }

        stopLatch.countDown();
      }
    }).start();

    final IndexedFloats indexed2 = supplier.get();
    try {
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
          }
          catch (InterruptedException e) {
            stopLatch.countDown();
            return;
          }

          try {
            for (int i = 0; i < numRuns; ++i) {
              for (int j = indexed2.size() - 1; j >= 0; --j) {
                final float val = vals[j];
                final float indexedVal = indexed2.get(j);
                if (Floats.compare(val, indexedVal) != 0) {
                  failureHappened.set(true);
                  reason.set(String.format("Thread2[%d]: %f != %f", j, val, indexedVal));
                  stopLatch.countDown();
                  return;
                }
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
            reason.set(e.getMessage());
            failureHappened.set(true);
          }

          stopLatch.countDown();
        }
      }).start();

      startLatch.countDown();

      stopLatch.await();
    }
    finally {
      indexed2.close();
    }

    if (failureHappened.get()) {
      Assert.fail("Failure happened.  Reason: " + reason.get());
    }
  }

  private void tryFill(final int startIndex, final int size)
  {
    float[] filled = new float[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i], 0.0);
    }
  }
}
