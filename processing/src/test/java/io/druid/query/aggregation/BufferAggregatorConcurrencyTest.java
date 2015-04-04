/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.segment.LongColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 *
 */
public class BufferAggregatorConcurrencyTest
{
  @Test
  public void testLongSumBufferAggregatorConcurrency() throws ExecutionException, InterruptedException
  {
    final int concurrentThreads = 8;
    final int tasksPerThread = 100000;
    final LongSumBufferAggregator agg = new LongSumBufferAggregator(
        new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return 1l;
          }
        }
    );
    final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.SIZE / 8 * 10);
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(concurrentThreads));
    final List<ListenableFuture<?>> futures = new LinkedList<>();

    for(int i = 0; i < concurrentThreads; ++i){
      futures.add(
          executorService.submit(
                      new Runnable()
                      {
                        @Override
                        public void run()
                        {
                          for(int i = 0; i < tasksPerThread; ++i){
                            synchronized (agg){
                              agg.aggregate(buffer, 4);
                            }
                          }
                        }
                      }
                  )
      );
    }
    Futures.allAsList(futures).get();
    Assert.assertEquals(tasksPerThread * concurrentThreads, agg.getLong(buffer, 4));
  }
}
