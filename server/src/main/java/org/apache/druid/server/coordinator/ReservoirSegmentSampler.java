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

package org.apache.druid.server.coordinator;

import com.google.common.math.IntMath;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.timeline.DataSegment;

import java.math.RoundingMode;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class ReservoirSegmentSampler
{
  private static final int SPLITERATOR_SIZE_THRESHOLD = 25;

  static BalancerSegmentHolder getRandomBalancerSegmentHolder(final List<ServerHolder> serverHolders)
  {
    ServerHolder fromServerHolder = reservoirSampleServer(serverHolders);
    if (fromServerHolder == null) {
      return null;
    }

    DataSegment proposalSegment = null;
    Collection<DataSegment> segments = fromServerHolder.getServer().getSegments();

    if (segments.size() == 1) {
      proposalSegment = segments.iterator().next();
    } else {
      Spliterator<DataSegment> chosenSpliterator = segments.spliterator();
      if (chosenSpliterator == null) {
        return null;
      }

      for (int i = 0; i < IntMath.log2(segments.size(), RoundingMode.UP); i++) {
        if (chosenSpliterator.estimateSize() < SPLITERATOR_SIZE_THRESHOLD) {
          List<DataSegment> finalList = StreamSupport
              .stream(chosenSpliterator, false)
              .collect(Collectors.toList());

          proposalSegment = getRandomElementFromList(finalList);
          break;
        }

        Spliterator<DataSegment> newSpliterator = chosenSpliterator.trySplit();

        // Choose between itself or the new spliterator with equal probability
        if (ThreadLocalRandom.current().nextBoolean()) {
          chosenSpliterator = newSpliterator;
        }
      }
    }
    return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
  }

  static ServerHolder reservoirSampleServer(final List<ServerHolder> serverHolders)
  {
    if (serverHolders.isEmpty()) {
      return null;
    }

    ServerHolder sampledServer = null;
    long total = 0;

    for (ServerHolder serverHolder : serverHolders) {
      ImmutableDruidServer server = serverHolder.getServer();
      long numSegments = server.getSegments().size();
      long endIndex = total + numSegments;
      // Handle edge case where first server contains no segments so that nextLong() doesn't fail
      long randomIndex = ThreadLocalRandom.current().nextLong((endIndex == 0) ? 1 : endIndex);

      // Select if random index falls within bounds of the segments contained in this server
      if (randomIndex >= total && randomIndex < endIndex) {
        sampledServer = serverHolder;
      }
      total += numSegments;
    }

    return sampledServer;
  }

  static <T> T getRandomElementFromList(List<T> list)
  {
    if (list.isEmpty()) {
      return null;
    }
    return list.get(ThreadLocalRandom.current().nextInt(list.size()));
  }

  private ReservoirSegmentSampler()
  {
  }
}
