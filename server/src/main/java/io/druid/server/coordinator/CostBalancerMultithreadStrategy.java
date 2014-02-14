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

package io.druid.server.coordinator;

import com.google.api.client.util.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class CostBalancerMultithreadStrategy extends AbstractCostBalancerStrategy
{
  private static final EmittingLogger log = new EmittingLogger(CostBalancerMultithreadStrategy.class);
  private final int threadCount;

  public CostBalancerMultithreadStrategy(DateTime referenceTimestamp, int threadCount)
  {
    super(referenceTimestamp);
    this.threadCount = threadCount;
  }

  protected Pair<Double, ServerHolder> chooseBestServer(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders,
      final boolean includeCurrentServer
  )
  {
    Pair<Double, ServerHolder> bestServer = Pair.of(Double.POSITIVE_INFINITY, null);

    ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    List<ListenableFuture<Pair<Double, ServerHolder>>> futures = Lists.newArrayList();

    for (final ServerHolder server : serverHolders) {
      futures.add(
          service.submit(
              new Callable<Pair<Double, ServerHolder>>()
              {
                @Override
                public Pair<Double, ServerHolder> call() throws Exception
                {
                  return Pair.of(computeCost(proposalSegment, server, includeCurrentServer), server);
                }
              }
          )
      );
    }

    final ListenableFuture<List<Pair<Double, ServerHolder>>> resultsFuture = Futures.allAsList(futures);

    try {
      for (Pair<Double, ServerHolder> server : resultsFuture.get()) {
        if (server.lhs < bestServer.lhs) {
          bestServer = server;
        }
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Cost Balancer Multithread strategy wasn't able to complete cost computation.").emit();
    }
    service.shutdown();
    return bestServer;
  }
}

