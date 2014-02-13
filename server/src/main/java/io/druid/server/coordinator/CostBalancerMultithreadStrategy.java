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
import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CostBalancerMultithreadStrategy extends AbstractCostBalancerStrategy
{
  private static final EmittingLogger log = new EmittingLogger(CostBalancerMultithreadStrategy.class);

  public CostBalancerMultithreadStrategy(DateTime referenceTimestamp)
  {
    super(referenceTimestamp);
  }

  protected Pair<Double, ServerHolder> chooseBestServer(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders,
      final boolean includeCurrentServer
  )
  {
    Pair<Double, ServerHolder> bestServer = Pair.of(Double.POSITIVE_INFINITY, null);

    ExecutorService service = Executors.newCachedThreadPool();
    List<Future<Pair<Double, ServerHolder>>> futures = Lists.newArrayList();

    for (final ServerHolder server : serverHolders) {
      futures.add(service.submit(new CostCalculator(server, proposalSegment, includeCurrentServer)));
    }

    for (Future<Pair<Double, ServerHolder>> f : futures) {
      try {
        Pair<Double, ServerHolder> server = f.get();
        if (server.lhs < bestServer.lhs) {
          bestServer = server;
        }
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
    service.shutdown();
    return bestServer;
  }

  private final class CostCalculator implements Callable<Pair<Double, ServerHolder>>
  {
    private final ServerHolder server;
    private final DataSegment proposalSegment;
    private final boolean includeCurrentServer;

    CostCalculator(final ServerHolder server, final DataSegment proposalSegment, final boolean includeCurrentServer)
    {
      this.server = server;
      this.proposalSegment = proposalSegment;
      this.includeCurrentServer = includeCurrentServer;
    }

    @Override
    public Pair<Double, ServerHolder> call() throws Exception
    {
      if (includeCurrentServer || !server.isServingSegment(proposalSegment)) {
        final long proposalSegmentSize = proposalSegment.getSize();
        /** Don't calculate cost if the server doesn't have enough space or is loading the segment */
        if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
          return Pair.of(Double.POSITIVE_INFINITY, null);
        }

        /** The contribution to the total cost of a given server by proposing to move the segment to that server is... */
        double cost = 0f;
        /**  the sum of the costs of other (exclusive of the proposalSegment) segments on the server */
        for (DataSegment segment : server.getServer().getSegments().values()) {
          if (!proposalSegment.equals(segment)) {
            cost += computeJointSegmentCosts(proposalSegment, segment);
          }
        }
        /**  plus the costs of segments that will be loaded */
        for (DataSegment segment : server.getPeon().getSegmentsToLoad()) {
          cost += computeJointSegmentCosts(proposalSegment, segment);
        }

        return Pair.of(cost, server);
      }
      return Pair.of(Double.POSITIVE_INFINITY, null);
    }
  }
}