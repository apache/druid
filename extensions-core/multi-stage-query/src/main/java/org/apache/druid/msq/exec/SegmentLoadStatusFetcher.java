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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class that periodically checks with the broker if all the segments generated are loaded by querying the sys table
 * and blocks till it is complete. This will account for and not wait for segments that would never be loaded due to
 * load rules. Should only be called if the query generates new segments or tombstones.
 * <br>
 * If an exception is thrown during operation, this will simply log the exception and exit without failing the task,
 * since the segments have already been published successfully, and should be loaded eventually.
 * <br>
 * If the segments are not loaded within {@link #TIMEOUT_DURATION_MILLIS} milliseconds, this logs a warning and exits
 * for the same reason.
 */
public class SegmentLoadStatusFetcher implements AutoCloseable
{
  private static final Logger log = new Logger(SegmentLoadStatusFetcher.class);
  private static final long SLEEP_DURATION_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long TIMEOUT_DURATION_MILLIS = TimeUnit.MINUTES.toMillis(10);

  /**
   * The query sent to the broker. This query uses replication_factor to determine how many copies of a segment has to be
   * loaded as per the load rules.
   * - If a segment is not used, the broker will not have any information about it, hence, a COUNT(*) should return the used count only.
   * - If replication_factor is more than 0, the segment will be loaded on historicals and needs to be waited for.
   * - If replication_factor is 0, that means that the segment will never be loaded on a historical and does not need to
   * be waited for.
   * - If replication_factor is -1, the replication factor is not known currently and will become known after a load rule
   * evaluation.
   * <br>
   * See <a href="https://github.com/apache/druid/pull/14403">this</a> for more details about replication_factor
   */
  private static final String LOAD_QUERY = "SELECT COUNT(*) AS usedSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_published = 1 AND replication_factor > 0) AS precachedSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_published = 1 AND replication_factor = 0) AS onDemandSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_available = 0 AND is_published = 1 AND replication_factor != 0) AS pendingSegments,\n"
                                           + "COUNT(*) FILTER (WHERE replication_factor = -1) AS unknownSegments\n"
                                           + "FROM sys.segments\n"
                                           + "WHERE datasource = '%s' AND is_overshadowed = 0 AND (%s)";

  private final BrokerClient brokerClient;
  private final ObjectMapper objectMapper;
  // Map of version vs latest load status.
  private final AtomicReference<VersionLoadStatus> versionLoadStatusReference;
  private final String datasource;
  private final String versionsConditionString;
  private final int totalSegmentsGenerated;
  private final boolean doWait;
  // since live reports fetch the value in another thread, we need to use AtomicReference
  private final AtomicReference<SegmentLoadWaiterStatus> status;

  private final ListeningExecutorService executorService;

  public SegmentLoadStatusFetcher(
      BrokerClient brokerClient,
      ObjectMapper objectMapper,
      String queryId,
      String datasource,
      Set<DataSegment> dataSegments,
      boolean doWait
  )
  {
    this.brokerClient = brokerClient;
    this.objectMapper = objectMapper;
    this.datasource = datasource;
    this.versionsConditionString = createVersionCondition(dataSegments);
    this.totalSegmentsGenerated = dataSegments.size();
    this.versionLoadStatusReference = new AtomicReference<>(new VersionLoadStatus(0, 0, 0, 0, totalSegmentsGenerated));
    this.status = new AtomicReference<>(new SegmentLoadWaiterStatus(
        State.INIT,
        null,
        0,
        totalSegmentsGenerated,
        0,
        0,
        0,
        0,
        totalSegmentsGenerated
    ));
    this.doWait = doWait;
    this.executorService = MoreExecutors.listeningDecorator(
        Execs.singleThreaded(StringUtils.encodeForFormat(queryId) + "-segment-load-waiter-%d")
    );
  }

  /**
   * Uses broker client to check if all segments created by the ingestion have been loaded and updates the {@link #status)}
   * periodically.
   * <br>
   * If an exception is thrown during operation, this will log the exception and return without failing the task,
   * since the segments have already been published successfully, and should be loaded eventually.
   * <br>
   * Only expected to be called from the main controller thread.
   */
  public void waitForSegmentsToLoad()
  {
    final DateTime startTime = DateTimes.nowUtc();
    final AtomicReference<Boolean> hasAnySegmentBeenLoaded = new AtomicReference<>(false);
    try {
      FutureUtils.getUnchecked(executorService.submit(() -> {
        long lastLogMillis = -TimeUnit.MINUTES.toMillis(1);
        try {
          while (!(hasAnySegmentBeenLoaded.get() && versionLoadStatusReference.get().isLoadingComplete())) {
            // Check the timeout and exit if exceeded.
            long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
            if (runningMillis > TIMEOUT_DURATION_MILLIS) {
              log.warn(
                  "Runtime[%d] exceeded timeout[%d] while waiting for segments to load. Exiting.",
                  runningMillis,
                  TIMEOUT_DURATION_MILLIS
              );
              updateStatus(State.TIMED_OUT, startTime);
              return;
            }

            if (runningMillis - lastLogMillis >= TimeUnit.MINUTES.toMillis(1)) {
              lastLogMillis = runningMillis;
              log.info(
                  "Fetching segment load status for datasource[%s] from broker",
                  datasource
              );
            }

            // Fetch the load status from the broker
            VersionLoadStatus loadStatus = fetchLoadStatusFromBroker();
            versionLoadStatusReference.set(loadStatus);
            hasAnySegmentBeenLoaded.set(hasAnySegmentBeenLoaded.get() || loadStatus.getUsedSegments() > 0);

            if (!(hasAnySegmentBeenLoaded.get() && versionLoadStatusReference.get().isLoadingComplete())) {
              // Update the status.
              updateStatus(State.WAITING, startTime);
              // Sleep for a bit before checking again.
              waitIfNeeded(SLEEP_DURATION_MILLIS);
            }
          }
        }
        catch (Exception e) {
          log.warn(e, "Exception occurred while waiting for segments to load. Exiting.");
          // Update the status and return.
          updateStatus(State.FAILED, startTime);
          return;
        }
        // Update the status.
        log.info("Segment loading completed for datasource[%s]", datasource);
        updateStatus(State.SUCCESS, startTime);
      }), true);
    }
    catch (Exception e) {
      log.warn(e, "Exception occurred while waiting for segments to load. Exiting.");
      updateStatus(State.FAILED, startTime);
    }
    finally {
      executorService.shutdownNow();
    }
  }
  private void waitIfNeeded(long waitTimeMillis) throws Exception
  {
    if (doWait) {
      Thread.sleep(waitTimeMillis);
    }
  }

  /**
   * Updates the {@link #status} with the latest details based on {@link #versionLoadStatusReference}
   */
  private void updateStatus(State state, DateTime startTime)
  {
    long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
    VersionLoadStatus versionLoadStatus = versionLoadStatusReference.get();
    status.set(
        new SegmentLoadWaiterStatus(
            state,
            startTime,
            runningMillis,
            totalSegmentsGenerated,
            versionLoadStatus.getUsedSegments(),
            versionLoadStatus.getPrecachedSegments(),
            versionLoadStatus.getOnDemandSegments(),
            versionLoadStatus.getPendingSegments(),
            versionLoadStatus.getUnknownSegments()
        )
    );
  }

  /**
   * Uses {@link #brokerClient} to fetch latest load status for a given set of versions. Converts the response into a
   * {@link VersionLoadStatus} and returns it.
   */
  private VersionLoadStatus fetchLoadStatusFromBroker() throws Exception
  {
    Request request = brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/");
    SqlQuery sqlQuery = new SqlQuery(StringUtils.format(LOAD_QUERY, datasource, versionsConditionString),
                                     ResultFormat.OBJECTLINES,
                                     false, false, false, null, null
    );
    request.setContent(MediaType.APPLICATION_JSON, objectMapper.writeValueAsBytes(sqlQuery));
    String response = brokerClient.sendQuery(request);

    if (response == null) {
      // Unable to query broker
      return new VersionLoadStatus(0, 0, 0, 0, totalSegmentsGenerated);
    } else if (response.trim().isEmpty()) {
      // If no segments are returned for a version, all segments have been dropped by a drop rule.
      return new VersionLoadStatus(0, 0, 0, 0, 0);
    } else {
      return objectMapper.readValue(response, VersionLoadStatus.class);
    }
  }

  /**
   * Takes a list of segments and creates the condition for the broker query. Directly creates a string to avoid
   * computing it repeatedly.
   */
  private static String createVersionCondition(Set<DataSegment> dataSegments)
  {
    // Creates a map of version to earliest and latest partition numbers created. These would be contiguous since the task
    // holds the lock.
    Map<String, Pair<Integer, Integer>> versionsVsPartitionNumberRangeMap = new HashMap<>();

    dataSegments.forEach(segment -> {
      final String version = segment.getVersion();
      final int partitionNum = segment.getId().getPartitionNum();
      versionsVsPartitionNumberRangeMap.computeIfPresent(version, (k, v) -> Pair.of(
          partitionNum < v.lhs ? partitionNum : v.lhs,
          partitionNum > v.rhs ? partitionNum : v.rhs
      ));
      versionsVsPartitionNumberRangeMap.computeIfAbsent(version, k -> Pair.of(partitionNum, partitionNum));
    });

    // Create a condition for each version / partition
    List<String> versionConditionList = new ArrayList<>();
    for (Map.Entry<String, Pair<Integer, Integer>> stringPairEntry : versionsVsPartitionNumberRangeMap.entrySet()) {
      Pair<Integer, Integer> pair = stringPairEntry.getValue();
      versionConditionList.add(
          StringUtils.format("(version = '%s' AND partition_num BETWEEN %s AND %s)", stringPairEntry.getKey(), pair.lhs, pair.rhs)
      );
    }
    return String.join(" OR ", versionConditionList);
  }

  /**
   * Returns the current status of the load.
   */
  public SegmentLoadWaiterStatus status()
  {
    return status.get();
  }

  @Override
  public void close()
  {
    try {
      executorService.shutdownNow();
    }
    catch (Throwable suppressed) {
      log.warn(suppressed, "Error shutting down SegmentLoadStatusFetcher");
    }
  }

  public static class SegmentLoadWaiterStatus
  {
    private final State state;
    private final DateTime startTime;
    private final long duration;
    private final int totalSegments;
    private final int usedSegments;
    private final int precachedSegments;
    private final int onDemandSegments;
    private final int pendingSegments;
    private final int unknownSegments;

    @JsonCreator
    public SegmentLoadWaiterStatus(
        @JsonProperty("state") SegmentLoadStatusFetcher.State state,
        @JsonProperty("startTime") @Nullable DateTime startTime,
        @JsonProperty("duration") long duration,
        @JsonProperty("totalSegments") int totalSegments,
        @JsonProperty("usedSegments") int usedSegments,
        @JsonProperty("precachedSegments") int precachedSegments,
        @JsonProperty("onDemandSegments") int onDemandSegments,
        @JsonProperty("pendingSegments") int pendingSegments,
        @JsonProperty("unknownSegments") int unknownSegments
    )
    {
      this.state = state;
      this.startTime = startTime;
      this.duration = duration;
      this.totalSegments = totalSegments;
      this.usedSegments = usedSegments;
      this.precachedSegments = precachedSegments;
      this.onDemandSegments = onDemandSegments;
      this.pendingSegments = pendingSegments;
      this.unknownSegments = unknownSegments;
    }

    @JsonProperty
    public SegmentLoadStatusFetcher.State getState()
    {
      return state;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    public long getDuration()
    {
      return duration;
    }

    @JsonProperty
    public long getTotalSegments()
    {
      return totalSegments;
    }

    @JsonProperty
    public int getUsedSegments()
    {
      return usedSegments;
    }

    @JsonProperty
    public int getPrecachedSegments()
    {
      return precachedSegments;
    }

    @JsonProperty
    public int getOnDemandSegments()
    {
      return onDemandSegments;
    }

    @JsonProperty
    public int getPendingSegments()
    {
      return pendingSegments;
    }

    @JsonProperty
    public int getUnknownSegments()
    {
      return unknownSegments;
    }
  }

  public enum State
  {
    /**
     * Initial state after being initialised with the segment versions and before #waitForSegmentsToLoad has been called.
     */
    INIT,
    /**
     * All segments that need to be loaded have not yet been loaded. The load status is perodically being queried from
     * the broker.
     */
    WAITING,
    /**
     * All segments which need to be loaded have been loaded, and the SegmentLoadWaiter exited successfully.
     */
    SUCCESS,
    /**
     * An exception occurred while checking load status. The SegmentLoadWaiter exited without failing the task.
     */
    FAILED,
    /**
     * The time spent waiting for segments to load exceeded org.apache.druid.msq.exec.SegmentLoadWaiter#TIMEOUT_DURATION_MILLIS.
     * The SegmentLoadWaiter exited without failing the task.
     */
    TIMED_OUT;

    public boolean isFinished()
    {
      return this == SUCCESS || this == FAILED || this == TIMED_OUT;
    }
  }

  public static class VersionLoadStatus
  {
    private final int usedSegments;
    private final int precachedSegments;
    private final int onDemandSegments;
    private final int pendingSegments;
    private final int unknownSegments;

    @JsonCreator
    public VersionLoadStatus(
        @JsonProperty("usedSegments") int usedSegments,
        @JsonProperty("precachedSegments") int precachedSegments,
        @JsonProperty("onDemandSegments") int onDemandSegments,
        @JsonProperty("pendingSegments") int pendingSegments,
        @JsonProperty("unknownSegments") int unknownSegments
    )
    {
      this.usedSegments = usedSegments;
      this.precachedSegments = precachedSegments;
      this.onDemandSegments = onDemandSegments;
      this.pendingSegments = pendingSegments;
      this.unknownSegments = unknownSegments;
    }

    @JsonProperty
    public int getUsedSegments()
    {
      return usedSegments;
    }

    @JsonProperty
    public int getPrecachedSegments()
    {
      return precachedSegments;
    }

    @JsonProperty
    public int getOnDemandSegments()
    {
      return onDemandSegments;
    }

    @JsonProperty
    public int getPendingSegments()
    {
      return pendingSegments;
    }

    @JsonProperty
    public int getUnknownSegments()
    {
      return unknownSegments;
    }

    @JsonIgnore
    public boolean isLoadingComplete()
    {
      return pendingSegments == 0 && (usedSegments == precachedSegments + onDemandSegments);
    }
  }
}
