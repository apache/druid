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
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
public class SegmentLoadWaiter
{
  private static final Logger log = new Logger(SegmentLoadWaiter.class);
  private static final long SLEEP_DURATION_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long TIMEOUT_DURATION_MILLIS = TimeUnit.MINUTES.toMillis(10);

  /**
   * The query sent to the broker. This query uses replication_factor to determine how many copies of a segment has to be
   * loaded as per the load rules.
   * - If a segment is not used, the broker will not have any information about it, hence, a COUNT(*) should return the used count only.
   * - If replication_factor is more than 0, the segment will be loaded on historicals and needs to be waited for.
   * - If replication_factor is 0, that means that the segment will never be loaded on a historical and does not need to
   *   be waited for.
   * - If replication_factor is -1, the replication factor is not known currently and will become known after a load rule
   *   evaluation.
   * <br>
   * See https://github.com/apache/druid/pull/14403 for more details about replication_factor
   */
  private static final String LOAD_QUERY = "SELECT COUNT(*) AS usedSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_published = 1 AND replication_factor > 0) AS precachedSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_published = 1 AND replication_factor = 0) AS onDemandSegments,\n"
                                           + "COUNT(*) FILTER (WHERE is_available = 0 AND is_published = 1 AND replication_factor != 0) AS pendingSegments,\n"
                                           + "COUNT(*) FILTER (WHERE replication_factor = -1) AS unknownSegments\n"
                                           + "FROM sys.segments\n"
                                           + "WHERE datasource = '%s' AND is_overshadowed = 0 AND version = '%s'";

  private final BrokerClient brokerClient;
  private final ObjectMapper objectMapper;
  // Map of version vs latest load status.
  private final Map<String, VersionLoadStatus> versionToLoadStatusMap;
  private final String datasource;
  private final Set<String> versionsToAwait;
  private final int totalSegmentsGenerated;
  private final boolean doWait;
  private final AtomicReference<SegmentLoadWaiterStatus> status;

  public SegmentLoadWaiter(
      BrokerClient brokerClient,
      ObjectMapper objectMapper,
      String datasource,
      Set<String> versionsToAwait,
      int totalSegmentsGenerated,
      boolean doWait
  )
  {
    this.brokerClient = brokerClient;
    this.objectMapper = objectMapper;
    this.datasource = datasource;
    this.versionsToAwait = new TreeSet<>(versionsToAwait);
    this.versionToLoadStatusMap = new HashMap<>();
    this.totalSegmentsGenerated = totalSegmentsGenerated;
    this.status = new AtomicReference<>(new SegmentLoadWaiterStatus(State.INIT, null, 0, totalSegmentsGenerated, 0, 0, 0, 0, totalSegmentsGenerated));
    this.doWait = doWait;
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
    DateTime startTime = DateTimes.nowUtc();
    boolean hasAnySegmentBeenLoaded = false;

    try {
      while (!versionsToAwait.isEmpty()) {
        // Check the timeout and exit if exceeded.
        long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
        if (runningMillis > TIMEOUT_DURATION_MILLIS) {
          log.warn("Runtime [%s] exceeded timeout [%s] while waiting for segments to load. Exiting.", runningMillis, TIMEOUT_DURATION_MILLIS);
          updateStatus(State.TIMED_OUT, startTime);
          return;
        }

        Iterator<String> iterator = versionsToAwait.iterator();

        // Query the broker for all pending versions
        while (iterator.hasNext()) {
          String version = iterator.next();

          // Fetch the load status for this version from the broker
          VersionLoadStatus loadStatus = fetchLoadStatusForVersion(version);
          versionToLoadStatusMap.put(version, loadStatus);

          hasAnySegmentBeenLoaded = hasAnySegmentBeenLoaded || loadStatus.getUsedSegments() > 0;

          // If loading is done for this stage, remove it from future loops.
          if (hasAnySegmentBeenLoaded && loadStatus.isLoadingComplete()) {
            iterator.remove();
          }
        }

        if (!versionsToAwait.isEmpty()) {
          // Update the status.
          updateStatus(State.WAITING, startTime);

          // Sleep for a while before retrying.
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
    updateStatus(State.SUCCESS, startTime);
  }

  private void waitIfNeeded(long waitTimeMillis) throws Exception
  {
    if (doWait) {
      Thread.sleep(waitTimeMillis);
    }
  }

  /**
   * Updates the {@link #status} with the latest details based on {@link #versionToLoadStatusMap}
   */
  private void updateStatus(State state, DateTime startTime)
  {
    int pendingSegmentCount = 0, usedSegmentsCount = 0, precachedSegmentCount = 0, onDemandSegmentCount = 0, unknownSegmentCount = 0;
    for (Map.Entry<String, VersionLoadStatus> entry : versionToLoadStatusMap.entrySet()) {
      usedSegmentsCount += entry.getValue().getUsedSegments();
      precachedSegmentCount += entry.getValue().getPrecachedSegments();
      onDemandSegmentCount += entry.getValue().getOnDemandSegments();
      unknownSegmentCount += entry.getValue().getUnknownSegments();
      pendingSegmentCount += entry.getValue().getPendingSegments();
    }

    long runningMillis = new Interval(startTime, DateTimes.nowUtc()).toDurationMillis();
    status.set(
        new SegmentLoadWaiterStatus(
            state,
            startTime,
            runningMillis,
            totalSegmentsGenerated,
            usedSegmentsCount,
            precachedSegmentCount,
            onDemandSegmentCount,
            pendingSegmentCount,
            unknownSegmentCount
        )
    );
  }

  /**
   * Uses {@link #brokerClient} to fetch latest load status for a given version. Converts the response into a
   * {@link VersionLoadStatus} and returns it.
   */
  private VersionLoadStatus fetchLoadStatusForVersion(String version) throws Exception
  {
    Request request = brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/");
    SqlQuery sqlQuery = new SqlQuery(StringUtils.format(LOAD_QUERY, datasource, version),
                                     ResultFormat.OBJECTLINES,
                                     false, false, false, null, null);
    request.setContent(MediaType.APPLICATION_JSON, objectMapper.writeValueAsBytes(sqlQuery));

    String response = brokerClient.sendQuery(request);

    if (response.trim().isEmpty()) {
      // If no segments are returned for a version, all segments have been dropped by a drop rule.
      return new VersionLoadStatus(0, 0, 0, 0, 0);
    } else {
      return objectMapper.readValue(response, VersionLoadStatus.class);
    }
  }

  /**
   * Returns the current status of the load.
   */
  public SegmentLoadWaiterStatus status()
  {
    return status.get();
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
        @JsonProperty("state") SegmentLoadWaiter.State state,
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
    public SegmentLoadWaiter.State getState()
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
    TIMED_OUT
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
