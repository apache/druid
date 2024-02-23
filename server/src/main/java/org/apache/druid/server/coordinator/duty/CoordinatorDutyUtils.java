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

package org.apache.druid.server.coordinator.duty;

import com.google.common.base.Predicate;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * utilty methods that are useful for coordinator duties
 */
public class CoordinatorDutyUtils
{

  private static final Logger LOG = new Logger(CoordinatorDutyUtils.class);

  /**
   * Returns the total worker capacity in the cluster, including autoscaling, if enabled.
   *
   * @param overlordClient The overlord client used to get worker capacity info.
   *
   * @return the total worker capacity in the cluster, including autoscaling, if enabled.
   */
  public static int getTotalWorkerCapacity(@Nonnull final OverlordClient overlordClient)
  {
    int totalWorkerCapacity;
    try {
      final IndexingTotalWorkerCapacityInfo workerCapacityInfo =
          FutureUtils.get(overlordClient.getTotalWorkerCapacity(), true);
      totalWorkerCapacity = workerCapacityInfo.getMaximumCapacityWithAutoScale();
      if (totalWorkerCapacity < 0) {
        totalWorkerCapacity = workerCapacityInfo.getCurrentClusterCapacity();
      }
    }
    catch (ExecutionException e) {
      // Call to getTotalWorkerCapacity may fail during a rolling upgrade: API was added in 0.23.0.
      if (e.getCause() instanceof HttpResponseException
          && ((HttpResponseException) e.getCause()).getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
        LOG.noStackTrace().warn(e, "Call to getTotalWorkerCapacity failed. Falling back to getWorkers.");
        totalWorkerCapacity =
            FutureUtils.getUnchecked(overlordClient.getWorkers(), true)
                .stream()
                .mapToInt(worker -> worker.getWorker().getCapacity())
                .sum();
      } else {
        throw new RuntimeException(e.getCause());
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    return totalWorkerCapacity;
  }

  /**
   * Return the number of active tasks that match the task predicate provided. The number of active tasks returned
   * may be an overestimate, as tasks that return status's with null types will be conservatively counted to match the
   * predicate provided.
   *
   * @param overlordClient The overlord client to use to retrieve the list of active tasks.
   * @param taskPredicate  The predicate to match against the list of retreived task status.
   *                       This predicate will never be called with a null task status.
   *
   * @return the number of active tasks that match the task predicate provided
   */
  public static List<TaskStatusPlus> getNumActiveTaskSlots(
      @Nonnull final OverlordClient overlordClient,
      final Predicate<TaskStatusPlus> taskPredicate
  )
  {
    final CloseableIterator<TaskStatusPlus> activeTasks =
        FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, 0), true);
    // Fetch currently running tasks that match the predicate
    List<TaskStatusPlus> taskStatuses = new ArrayList<>();

    try (final Closer closer = Closer.create()) {
      closer.register(activeTasks);
      while (activeTasks.hasNext()) {
        final TaskStatusPlus status = activeTasks.next();

        // taskType can be null if middleManagers are running with an older version. Here, we consevatively regard
        // the tasks of the unknown taskType as the killTask. This is because it's important to not run
        // killTasks more than the configured limit at any time which might impact to the ingestion
        // performance.
        if (null != status && (null == status.getType() || (taskPredicate.apply(status)))) {
          taskStatuses.add(status);
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return taskStatuses;
  }
}
