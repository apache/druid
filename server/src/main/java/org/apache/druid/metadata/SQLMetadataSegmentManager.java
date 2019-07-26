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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.BaseResultSetMapper;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class SQLMetadataSegmentManager implements MetadataSegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataSegmentManager.class);

  /**
   * Use to synchronize {@link #start()}, {@link #stop()}, {@link #poll()}, and {@link #isStarted()}. These methods
   * should be synchronized to prevent from being called at the same time if two different threads are calling them.
   * This might be possible if a druid coordinator gets and drops leadership repeatedly in quick succession.
   */
  private final ReentrantReadWriteLock startStopLock = new ReentrantReadWriteLock();

  /**
   * Used to ensure that {@link #poll()} is never run concurrently. It should already be so (at least in production
   * code), where {@link #poll()} is called only from the task created in {@link #createPollTaskForStartOrder} and is
   * scheduled in a single-threaded {@link #exec}, so this lock is an additional safety net in case there are bugs in
   * the code, and for tests, where {@link #poll()} is called from the outside code.
   *
   * Not using {@link #startStopLock}.writeLock() in order to still be able to run {@link #poll()} concurrently with
   * {@link #isStarted()}.
   */
  private final Object pollLock = new Object();

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;

  // Volatile since this reference is reassigned in "poll" and then read from in other threads.
  // Starts null so we can differentiate "never polled" (null) from "polled, but empty" (empty dataSources map and
  // empty overshadowedSegments set).
  // Note that this is not simply a lazy-initialized variable: it starts off as null, and may transition between
  // null and nonnull multiple times as stop() and start() are called.
  @Nullable
  private volatile DataSourcesSnapshot dataSourcesSnapshot = null;

  /**
   * The number of times this SQLMetadataSegmentManager was started.
   */
  private long startCount = 0;
  /**
   * Equal to the current {@link #startCount} value, if the SQLMetadataSegmentManager is currently started; -1 if
   * currently stopped.
   *
   * This field is used to implement a simple stamp mechanism instead of just a boolean "started" flag to prevent
   * the theoretical situation of two or more tasks scheduled in {@link #start()} calling {@link #isStarted()} and
   * {@link #poll()} concurrently, if the sequence of {@link #start()} - {@link #stop()} - {@link #start()} actions
   * occurs quickly.
   *
   * {@link SQLMetadataRuleManager} also have a similar issue.
   */
  private long currentStartOrder = -1;
  private ScheduledExecutorService exec = null;

  @Inject
  public SQLMetadataSegmentManager(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.connector = connector;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopLock.writeLock();
    lock.lock();
    try {
      if (isStarted()) {
        return;
      }

      startCount++;
      currentStartOrder = startCount;
      final long localStartOrder = currentStartOrder;

      exec = Execs.scheduledSingleThreaded("DatabaseSegmentManager-Exec--%d");

      final Duration delay = config.get().getPollDuration().toStandardDuration();
      exec.scheduleWithFixedDelay(
          createPollTaskForStartOrder(localStartOrder),
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
    finally {
      lock.unlock();
    }
  }

  private Runnable createPollTaskForStartOrder(long startOrder)
  {
    return () -> {
      // poll() is synchronized together with start(), stop() and isStarted() to ensure that when stop() exits, poll()
      // won't actually run anymore after that (it could only enter the syncrhonized section and exit immediately
      // because the localStartedOrder doesn't match the new currentStartOrder). It's needed to avoid flakiness in
      // SQLMetadataSegmentManagerTest. See https://github.com/apache/incubator-druid/issues/6028
      ReentrantReadWriteLock.ReadLock lock = startStopLock.readLock();
      lock.lock();
      try {
        if (startOrder == currentStartOrder) {
          poll();
        } else {
          log.debug("startOrder = currentStartOrder = %d, skipping poll()", startOrder);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "uncaught exception in segment manager polling thread").emit();
      }
      finally {
        lock.unlock();
      }
    };
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    ReentrantReadWriteLock.WriteLock lock = startStopLock.writeLock();
    lock.lock();
    try {
      if (!isStarted()) {
        return;
      }
      dataSourcesSnapshot = null;
      currentStartOrder = -1;
      exec.shutdownNow();
      exec = null;
    }
    finally {
      lock.unlock();
    }
  }

  private Pair<DataSegment, Boolean> usedPayloadMapper(
      final int index,
      final ResultSet resultSet,
      final StatementContext context
  ) throws SQLException
  {
    try {
      return new Pair<>(
          jsonMapper.readValue(resultSet.getBytes("payload"), DataSegment.class),
          resultSet.getBoolean("used")
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a list of all datasegments that overlap the provided interval along with thier used status.
   */
  private List<Pair<DataSegment, Boolean>> getDataSegmentsOverlappingInterval(
      final String dataSource,
      final Interval interval
  )
  {
    return connector.inReadOnlyTransaction(
        (handle, status) -> handle.createQuery(
            StringUtils.format(
                "SELECT used, payload FROM %1$s WHERE dataSource = :dataSource AND start < :end AND %2$send%2$s > :start",
                getSegmentsTable(),
                connector.getQuoteString()
            )
        )
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("dataSource", dataSource)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .map(this::usedPayloadMapper)
        .list()
    );
  }

  private List<Pair<DataSegment, Boolean>> getDataSegments(
      final String dataSource,
      final Collection<String> segmentIds,
      final Handle handle
  )
  {
    return segmentIds.stream().map(
        segmentId -> Optional.ofNullable(
            handle.createQuery(
                StringUtils.format(
                    "SELECT used, payload FROM %1$s WHERE dataSource = :dataSource AND id = :id",
                    getSegmentsTable()
                )
            )
            .bind("dataSource", dataSource)
            .bind("id", segmentId)
            .map(this::usedPayloadMapper)
            .first()
        )
        .orElseThrow(() -> new UnknownSegmentIdException(StringUtils.format("Cannot find segment id [%s]", segmentId)))
    )
    .collect(Collectors.toList());
  }

  /**
   * Builds a VersionedIntervalTimeline containing used segments that overlap the intervals passed.
   */
  private VersionedIntervalTimeline<String, DataSegment> buildVersionedIntervalTimeline(
      final String dataSource,
      final Collection<Interval> intervals,
      final Handle handle
  )
  {
    return VersionedIntervalTimeline.forSegments(intervals
        .stream()
        .flatMap(interval -> handle.createQuery(
                StringUtils.format(
                    "SELECT payload FROM %1$s WHERE dataSource = :dataSource AND start < :end AND %2$send%2$s > :start AND used = true",
                    getSegmentsTable(),
                    connector.getQuoteString()
                )
            )
            .setFetchSize(connector.getStreamingFetchSize())
            .bind("dataSource", dataSource)
            .bind("start", interval.getStart().toString())
            .bind("end", interval.getEnd().toString())
            .map((i, resultSet, context) -> {
              try {
                return jsonMapper.readValue(resultSet.getBytes("payload"), DataSegment.class);
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
            .list()
            .stream()
        )
        .iterator()
    );
  }

  @Override
  public boolean enableDataSource(final String dataSource)
  {
    try {
      return enableSegments(dataSource, Intervals.ETERNITY) != 0;
    }
    catch (Exception e) {
      log.error(e, "Exception enabling datasource %s", dataSource);
      return false;
    }
  }

  @Override
  public int enableSegments(final String dataSource, final Interval interval)
  {
    List<Pair<DataSegment, Boolean>> segments = getDataSegmentsOverlappingInterval(dataSource, interval);
    List<DataSegment> segmentsToEnable = segments.stream()
        .filter(segment -> !segment.rhs && interval.contains(segment.lhs.getInterval()))
        .map(segment -> segment.lhs)
        .collect(Collectors.toList());

    VersionedIntervalTimeline<String, DataSegment> versionedIntervalTimeline = VersionedIntervalTimeline.forSegments(
        segments.stream().filter(segment -> segment.rhs).map(segment -> segment.lhs).iterator()
    );
    VersionedIntervalTimeline.addSegments(versionedIntervalTimeline, segmentsToEnable.iterator());

    return enableSegments(
        segmentsToEnable,
        versionedIntervalTimeline
    );
  }

  @Override
  public int enableSegments(final String dataSource, final Collection<String> segmentIds)
  {
    Pair<List<DataSegment>, VersionedIntervalTimeline<String, DataSegment>> data = connector.inReadOnlyTransaction(
        (handle, status) -> {
          List<DataSegment> segments = getDataSegments(dataSource, segmentIds, handle)
              .stream()
              .filter(pair -> !pair.rhs)
              .map(pair -> pair.lhs)
              .collect(Collectors.toList());

          VersionedIntervalTimeline<String, DataSegment> versionedIntervalTimeline = buildVersionedIntervalTimeline(
              dataSource,
              JodaUtils.condenseIntervals(segments.stream().map(segment -> segment.getInterval()).collect(Collectors.toList())),
              handle
          );
          VersionedIntervalTimeline.addSegments(versionedIntervalTimeline, segments.iterator());

          return new Pair<>(
              segments,
              versionedIntervalTimeline
          );
        }
    );

    return enableSegments(
        data.lhs,
        data.rhs
    );
  }

  private int enableSegments(
      final Collection<DataSegment> segments,
      final VersionedIntervalTimeline<String, DataSegment> versionedIntervalTimeline
  )
  {
    if (segments.isEmpty()) {
      log.warn("No segments found to update!");
      return 0;
    }

    return connector.getDBI().withHandle(handle -> {
      Batch batch = handle.createBatch();
      segments
          .stream()
          .map(segment -> segment.getId())
          .filter(segmentId -> !versionedIntervalTimeline.isOvershadowed(
              segmentId.getInterval(),
              segmentId.getVersion()
          ))
          .forEach(segmentId -> batch.add(
              StringUtils.format(
                  "UPDATE %s SET used=true WHERE id = '%s'",
                  getSegmentsTable(),
                  segmentId
              )
          ));
      return batch.execute().length;
    });
  }

  @Override
  public boolean enableSegment(final String segmentId)
  {
    try {
      connector.getDBI().withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              handle.createStatement(StringUtils.format("UPDATE %s SET used=true WHERE id = :id", getSegmentsTable()))
                    .bind("id", segmentId)
                    .execute();
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, "Exception enabling segment %s", segmentId);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeDataSource(final String dataSource)
  {
    try {
      final int removed = connector.getDBI().withHandle(
          handle -> handle.createStatement(
              StringUtils.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", getSegmentsTable())
          ).bind("dataSource", dataSource).execute()
      );

      if (removed == 0) {
        return false;
      }
    }
    catch (Exception e) {
      log.error(e, "Error removing datasource %s", dataSource);
      return false;
    }

    return true;
  }

  /**
   * This method does not update {@code dataSourcesSnapshot}, see the comments in {@code doPoll()} about
   * snapshot update. The segment removal will be reflected after next poll cyccle runs.
   */
  @Override
  public boolean removeSegment(String segmentId)
  {
    try {
      return removeSegmentFromTable(segmentId);
    }
    catch (Exception e) {
      log.error(e, e.toString());
      return false;
    }
  }

  @Override
  public long disableSegments(String dataSource, Collection<String> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      return 0;
    }
    final long[] result = new long[1];
    try {
      connector.getDBI().withHandle(handle -> {
        Batch batch = handle.createBatch();
        segmentIds
            .forEach(segmentId -> batch.add(
                StringUtils.format(
                    "UPDATE %s SET used=false WHERE datasource = '%s' AND id = '%s' ",
                    getSegmentsTable(),
                    dataSource,
                    segmentId
                )
            ));
        final int[] resultArr = batch.execute();
        result[0] = Arrays.stream(resultArr).filter(x -> x > 0).count();
        return result[0];
      });
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result[0];
  }

  @Override
  public int disableSegments(String dataSource, Interval interval)
  {
    try {
      return connector.getDBI().withHandle(
          handle -> handle
              .createStatement(
                  StringUtils
                      .format(
                          "UPDATE %s SET used=false WHERE datasource = :datasource "
                          + "AND start >= :start AND %2$send%2$s <= :end",
                          getSegmentsTable(),
                          connector.getQuoteString()
                      ))
              .bind("datasource", dataSource)
              .bind("start", interval.getStart().toString())
              .bind("end", interval.getEnd().toString())
              .execute()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean removeSegmentFromTable(String segmentId)
  {
    final int removed = connector.getDBI().withHandle(
        handle -> handle
            .createStatement(StringUtils.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable()))
            .bind("segmentID", segmentId)
            .execute()
    );
    return removed > 0;
  }

  @Override
  public boolean isStarted()
  {
    // isStarted() is synchronized together with start(), stop() and poll() to ensure that the latest currentStartOrder
    // is always visible. readLock should be used to avoid unexpected performance degradation of DruidCoordinator.
    ReentrantReadWriteLock.ReadLock lock = startStopLock.readLock();
    lock.lock();
    try {
      return currentStartOrder >= 0;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  @Nullable
  public ImmutableDruidDataSource getDataSource(String dataSourceName)
  {
    final ImmutableDruidDataSource dataSource = Optional.ofNullable(dataSourcesSnapshot)
                                                        .map(m -> m.getDataSourcesMap().get(dataSourceName))
                                                        .orElse(null);
    return dataSource == null ? null : dataSource;
  }

  @Override
  @Nullable
  public Collection<ImmutableDruidDataSource> getDataSources()
  {
    return Optional.ofNullable(dataSourcesSnapshot).map(m -> m.getDataSources()).orElse(null);
  }

  @Override
  @Nullable
  public Iterable<DataSegment> iterateAllSegments()
  {
    final Collection<ImmutableDruidDataSource> dataSources = Optional.ofNullable(dataSourcesSnapshot)
                                                                     .map(m -> m.getDataSources())
                                                                     .orElse(null);
    if (dataSources == null) {
      return null;
    }

    return () -> dataSources.stream()
                            .flatMap(dataSource -> dataSource.getSegments().stream())
                            .iterator();
  }

  @Override
  @Nullable
  public Set<SegmentId> getOvershadowedSegments()
  {
    return Optional.ofNullable(dataSourcesSnapshot).map(m -> m.getOvershadowedSegments()).orElse(null);
  }

  @Nullable
  @Override
  public DataSourcesSnapshot getDataSourcesSnapshot()
  {
    return dataSourcesSnapshot;
  }

  @Override
  public Collection<String> getAllDataSourceNames()
  {
    return connector.getDBI().withHandle(
        handle -> handle.createQuery(
            StringUtils.format("SELECT DISTINCT(datasource) FROM %s", getSegmentsTable())
        )
                        .fold(
                            new ArrayList<>(),
                            new Folder3<List<String>, Map<String, Object>>()
                            {
                              @Override
                              public List<String> fold(
                                  List<String> druidDataSources,
                                  Map<String, Object> stringObjectMap,
                                  FoldController foldController,
                                  StatementContext statementContext
                              )
                              {
                                druidDataSources.add(
                                    MapUtils.getString(stringObjectMap, "datasource")
                                );
                                return druidDataSources;
                              }
                            }
                        )
    );
  }

  @Override
  public void poll()
  {
    // See the comment to the pollLock field, explaining this synchronized block
    synchronized (pollLock) {
      try {
        doPoll();
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling DB.").emit();
      }
    }
  }

  private void doPoll()
  {
    log.debug("Starting polling of segment table");

    // some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    //
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        new TransactionCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> inTransaction(Handle handle, TransactionStatus status)
          {
            return handle
                .createQuery(StringUtils.format("SELECT payload FROM %s WHERE used=true", getSegmentsTable()))
                .setFetchSize(connector.getStreamingFetchSize())
                .map(
                    new ResultSetMapper<DataSegment>()
                    {
                      @Override
                      public DataSegment map(int index, ResultSet r, StatementContext ctx) throws SQLException
                      {
                        try {
                          DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);
                          return DataSegment.intern(segment, true);
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Failed to read segment from db.").emit();
                          return null;
                        }
                      }
                    }
                )
                .list();
          }
        }
    );

    if (segments == null || segments.isEmpty()) {
      log.warn("No segments found in the database!");
      return;
    }

    log.info("Polled and found %,d segments in the database", segments.size());

    ConcurrentHashMap<String, DruidDataSource> newDataSources = new ConcurrentHashMap<>();

    ImmutableMap<String, String> dataSourceProperties = ImmutableMap.of("created", DateTimes.nowUtc().toString());
    segments
        .stream()
        .filter(Objects::nonNull)
        .forEach(segment -> {
          newDataSources
              .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
              .addSegmentIfAbsent(segment);
        });

    // dataSourcesSnapshot is updated only here, please note that if datasources or segments are enabled or disabled
    // outside of poll, the dataSourcesSnapshot can become invalid until the next poll cycle.
    // DataSourcesSnapshot computes the overshadowed segments, which makes it an expensive operation if the
    // snapshot is invalidated on each segment removal, especially if a user issues a lot of single segment remove
    // calls in rapid succession. So the snapshot update is not done outside of poll at this time.
    // Updates outside of poll(), were primarily for the user experience, so users would immediately see the effect of
    // a segment remove call reflected in MetadataResource API calls. These updates outside of scheduled poll may be
    // added back in removeDataSource and removeSegment methods after the on-demand polling changes from
    // https://github.com/apache/incubator-druid/pull/7653 are in.
    final Map<String, ImmutableDruidDataSource> updatedDataSources = CollectionUtils.mapValues(
        newDataSources,
        v -> v.toImmutableDruidDataSource()
    );
    dataSourcesSnapshot = new DataSourcesSnapshot(updatedDataSources);
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      final Interval interval,
      final int limit
  )
  {
    return connector.inReadOnlyTransaction(
        new TransactionCallback<List<Interval>>()
        {
          @Override
          public List<Interval> inTransaction(Handle handle, TransactionStatus status)
          {
            Iterator<Interval> iter = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT start, %2$send%2$s FROM %1$s WHERE dataSource = :dataSource and start >= :start and %2$send%2$s <= :end and used = false ORDER BY start, %2$send%2$s",
                        getSegmentsTable(),
                        connector.getQuoteString()
                    )
                )
                .setFetchSize(connector.getStreamingFetchSize())
                .setMaxRows(limit)
                .bind("dataSource", dataSource)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .map(
                    new BaseResultSetMapper<Interval>()
                    {
                      @Override
                      protected Interval mapInternal(int index, Map<String, Object> row)
                      {
                        return new Interval(
                            DateTimes.of((String) row.get("start")),
                            DateTimes.of((String) row.get("end"))
                        );
                      }
                    }
                )
                .iterator();


            List<Interval> result = Lists.newArrayListWithCapacity(limit);
            for (int i = 0; i < limit && iter.hasNext(); i++) {
              try {
                result.add(iter.next());
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            return result;
          }
        }
    );
  }
}
