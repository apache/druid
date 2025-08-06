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

package org.apache.druid.testing.embedded;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Contains various utility methods to interact with an {@link EmbeddedDruidCluster}.
 *
 * @see #onLeaderCoordinator(Function)
 * @see #onLeaderOverlord(Function)
 * @see #runSql(String, Object...)
 */
public class EmbeddedClusterApis
{
  private final EmbeddedDruidCluster cluster;

  EmbeddedClusterApis(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  public <T> T onLeaderCoordinator(Function<CoordinatorClient, ListenableFuture<T>> coordinatorApi)
  {
    return getResult(coordinatorApi.apply(cluster.leaderCoordinator()));
  }

  public <T> T onLeaderCoordinatorSync(Function<CoordinatorClient, T> coordinatorApi)
  {
    return coordinatorApi.apply(cluster.leaderCoordinator());
  }

  public <T> T onLeaderOverlord(Function<OverlordClient, ListenableFuture<T>> overlordApi)
  {
    return getResult(overlordApi.apply(cluster.leaderOverlord()));
  }

  /**
   * Submits the given SQL query to any of the brokers (using {@code BrokerClient})
   * of the cluster.
   *
   * @return The result of the SQL as a single CSV string.
   */
  public String runSql(String sql, Object... args)
  {
    try {
      return getResult(
          cluster.anyBroker().submitSqlQuery(
              new ClientSqlQuery(
                  StringUtils.format(sql, args),
                  ResultFormat.CSV.name(),
                  false,
                  false,
                  false,
                  null,
                  null
              )
          )
      ).trim();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Runs the given SQL query for a datasource and verifies the result.
   *
   * @param query             Must contain a {@code %s} placeholder for the datasource.
   * @param dataSource        Datasource for which the query should be run.
   * @param expectedResultCsv Expected result as a CSV String.
   */
  public void verifySqlQuery(String query, String dataSource, String expectedResultCsv)
  {
    Assertions.assertEquals(
        expectedResultCsv,
        cluster.runSql(query, dataSource),
        StringUtils.format("Query[%s] failed", query)
    );
  }

  /**
   * Runs a {@link Task} on this cluster and waits until it has completed successfully.
   * The given {@link EmbeddedOverlord} must be the leader with a {@code LatchableEmitter}
   * bound so that the task completion metric can be waited upon.
   */
  public void runTask(Task task, EmbeddedOverlord leaderOverlord)
  {
    final String taskId = task.getId();
    submitTask(task);
    waitForTaskToSucceed(taskId, leaderOverlord);
  }

  /**
   * Submits a {@link Task} to the leader Overlord but does not wait for it to finish.
   * Shorthand for {@code onLeaderOverlord(o -> o.runTask(task.getId(), task))}.
   */
  public void submitTask(Task task)
  {
    onLeaderOverlord(o -> o.runTask(task.getId(), task));
  }

  /**
   * Waits for the given task to finish successfully. If the given
   * {@link EmbeddedOverlord} is not the leader, this method can only return by
   * throwing an exception upon timeout.
   */
  public void waitForTaskToSucceed(String taskId, EmbeddedOverlord overlord)
  {
    Assertions.assertEquals(
        TaskState.SUCCESS,
        waitForTaskToFinish(taskId, overlord).getStatusCode()
    );
  }

  /**
   * Waits for the given task to finish (either successfully or unsuccessfully). If the given
   * {@link EmbeddedOverlord} is not the leader, this method can only return by
   * throwing an exception upon timeout.
   */
  public TaskStatus waitForTaskToFinish(String taskId, EmbeddedOverlord overlord)
  {
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(TaskMetrics.RUN_DURATION)
                      .hasDimension(DruidMetrics.TASK_ID, taskId)
    );
    return getTaskStatus(taskId);
  }

  /**
   * Retrieves all used segments from the metadata store (or cache if applicable).
   */
  public Set<DataSegment> getVisibleUsedSegments(String dataSource, EmbeddedOverlord overlord)
  {
    return overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE);
  }

  /**
   * Returns intervals of all visible used segments sorted using the
   * {@link Comparators#intervalsByStartThenEnd()}.
   */
  public List<Interval> getSortedSegmentIntervals(String dataSource, EmbeddedOverlord overlord)
  {
    final Comparator<Interval> comparator = Comparators.intervalsByStartThenEnd().reversed();
    final TreeSet<Interval> sortedIntervals = new TreeSet<>(comparator);

    final Set<DataSegment> allUsedSegments = getVisibleUsedSegments(dataSource, overlord);
    for (DataSegment segment : allUsedSegments) {
      sortedIntervals.add(segment.getInterval());
    }

    return new ArrayList<>(sortedIntervals);
  }

  /**
   * Verifies that the number of visible used segments is the same as expected.
   */
  public void verifyNumVisibleSegmentsIs(int numExpectedSegments, String dataSource, EmbeddedOverlord overlord)
  {
    int segmentCount = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).size();
    Assertions.assertEquals(
        numExpectedSegments,
        segmentCount,
        "Segment count mismatch"
    );
    Assertions.assertEquals(
        String.valueOf(segmentCount),
        cluster.runSql(
            "SELECT COUNT(*) FROM sys.segments WHERE datasource='%s'"
            + " AND is_overshadowed = 0 AND is_available = 1",
            dataSource
        ),
        "Segment count mismatch in sys.segments table"
    );
  }

  /**
   * Waits for all used segments (including overshadowed) of the given datasource
   * to be loaded on historicals.
   */
  public void waitForAllSegmentsToBeAvailable(String dataSource, EmbeddedCoordinator coordinator)
  {
    final int numSegments = coordinator
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED)
        .size();
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(numSegments)
    );
  }

  /**
   * Returns a {@link Closeable} that deletes all the data for the given datasource
   * on {@link Closeable#close()}.
   */
  public Closeable createUnloader(String dataSource)
  {
    return () -> onLeaderOverlord(o -> o.markSegmentsAsUnused(dataSource));
  }

  /**
   * Fetches the status of the given task from the cluster and verifies that it
   * matches the expected status.
   */
  public void verifyTaskHasStatus(String taskId, TaskStatus expectedStatus)
  {
    final TaskStatusResponse currentStatus = onLeaderOverlord(
        o -> o.taskStatus(taskId)
    );
    Assertions.assertNotNull(currentStatus.getStatus());
    Assertions.assertEquals(
        expectedStatus.getStatusCode(),
        currentStatus.getStatus().getStatusCode(),
        StringUtils.format("Task[%s] has unexpected status", taskId)
    );
    Assertions.assertEquals(
        expectedStatus.getErrorMsg(),
        currentStatus.getStatus().getErrorMsg(),
        StringUtils.format("Task[%s] has unexpected error message", taskId)
    );
  }

  /**
   * Gets the current status of the given task.
   */
  public TaskStatus getTaskStatus(String taskId)
  {
    final TaskStatusPlus statusPlus = onLeaderOverlord(o -> o.taskStatus(taskId)).getStatus();
    Assertions.assertNotNull(statusPlus);

    return new TaskStatus(
        statusPlus.getId(),
        Objects.requireNonNull(statusPlus.getStatusCode()),
        Objects.requireNonNull(statusPlus.getDuration()),
        statusPlus.getErrorMsg(),
        statusPlus.getLocation()
    );
  }

  /**
   * Posts the given supervisor to the leader Overlord of this cluster.
   * Shorhand for {@code onLeaderOverlord(o -> o.postSupervisor(supervisor)).get("id")}.
   *
   * @return ID of the submitted supervisor
   */
  public String postSupervisor(SupervisorSpec supervisor)
  {
    return onLeaderOverlord(o -> o.postSupervisor(supervisor)).get("id");
  }

  /**
   * Fetches the current status of the given supervisor ID.
   */
  public SupervisorStatus getSupervisorStatus(String supervisorId)
  {
    final List<SupervisorStatus> supervisors = ImmutableList.copyOf(
        onLeaderOverlord(OverlordClient::supervisorStatuses)
    );
    for (SupervisorStatus supervisor : supervisors) {
      if (supervisor.getId().equals(supervisorId)) {
        return supervisor;
      }
    }

    throw new ISE("Could not find supervisor[%s]", supervisorId);
  }

  // STATIC UTILITY METHODS

  /**
   * Creates a random datasource name prefixed with {@link TestDataSource#WIKI}.
   */
  public static String createTestDatasourceName()
  {
    return TestDataSource.WIKI + "_" + IdUtils.getRandomId();
  }

  /**
   * Creates a random task ID prefixed with the {@code dataSource}.
   */
  public static String newTaskId(String dataSource)
  {
    return dataSource + "_" + IdUtils.getRandomId();
  }

  /**
   * Deserializes the given JSON string into a generic map that can be used to
   * post JSON payloads to a Druid API. Using a generic map allows the client
   * to make requests even if required types are not loaded.
   */
  public static Map<String, Object> deserializeJsonToMap(String payload)
  {
    try {
      return TestHelper.JSON_MAPPER.readValue(payload, new TypeReference<>() {});
    }
    catch (Exception e) {
      throw new ISE(e, "Could not deserialize payload[%s]", payload);
    }
  }

  /**
   * Creates a list of intervals that align with the given target granularity
   * and overlap the original list of given intervals. If the original list is
   * sorted, the returned list would be sorted too.
   */
  public static List<Interval> createAlignedIntervals(
      List<Interval> original,
      Granularity targetGranularity
  )
  {
    final List<Interval> alignedIntervals = new ArrayList<>();
    for (Interval interval : original) {
      for (Interval alignedInterval :
          targetGranularity.getIterable(new Interval(interval, ISOChronology.getInstanceUTC()))) {
        alignedIntervals.add(alignedInterval);
      }
    }

    return alignedIntervals;
  }

  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }

  @FunctionalInterface
  public interface TaskBuilder
  {
    Object build(String dataSource, String taskId);
  }
}
