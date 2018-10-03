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
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.BaseResultSetMapper;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
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
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  /** {@link #poll()} and {@link #isStarted()} use readLock. */
  private final Lock readLock = readWriteLock.readLock();
  /** {@link #start()} and {@link #stop()} use writeLock. */
  private final Lock writeLock = readWriteLock.writeLock();

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final AtomicReference<ConcurrentHashMap<String, DruidDataSource>> dataSourcesRef;
  private final SQLMetadataConnector connector;

  /** The number of times this SQLMetadataSegmentManager was started. */
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
    this.dataSourcesRef = new AtomicReference<>(new ConcurrentHashMap<>());
    this.connector = connector;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    writeLock.lock();
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
          new Runnable()
          {
            @Override
            public void run()
            {
              // poll() is synchronized together with start(), stop() and isStarted() to ensure that when stop() exists,
              // poll() won't actually run anymore after that (it could only enter the syncrhonized section and exit
              // immediately because the localStartedOrder doesn't match the new currentStartOrder). It's needed
              // to avoid flakiness in SQLMetadataSegmentManagerTest.
              // See https://github.com/apache/incubator-druid/issues/6028
              readLock.lock();
              try {
                if (localStartOrder == currentStartOrder) {
                  poll();
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "uncaught exception in segment manager polling thread").emit();

              }
              finally {
                readLock.unlock();
              }
            }
          },
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    writeLock.lock();
    try {
      if (!isStarted()) {
        return;
      }

      final ConcurrentHashMap<String, DruidDataSource> emptyMap = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, DruidDataSource> current;
      do {
        current = dataSourcesRef.get();
      } while (!dataSourcesRef.compareAndSet(current, emptyMap));

      currentStartOrder = -1;
      exec.shutdownNow();
      exec = null;
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean enableDatasource(final String ds)
  {
    try {
      final IDBI dbi = connector.getDBI();
      VersionedIntervalTimeline<String, DataSegment> segmentTimeline = connector.inReadOnlyTransaction(
          (handle, status) -> VersionedIntervalTimeline.forSegments(
              Iterators.transform(
                  handle
                      .createQuery(
                          StringUtils.format(
                              "SELECT payload FROM %s WHERE dataSource = :dataSource",
                              getSegmentsTable()
                          )
                      )
                      .setFetchSize(connector.getStreamingFetchSize())
                      .bind("dataSource", ds)
                      .map(ByteArrayMapper.FIRST)
                      .iterator(),
                  payload -> {
                    try {
                      return jsonMapper.readValue(payload, DataSegment.class);
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              )

          )
      );

      final List<DataSegment> segments = Lists.newArrayList();
      List<TimelineObjectHolder<String, DataSegment>> timelineObjectHolders = segmentTimeline.lookup(
          Intervals.of("0000-01-01/3000-01-01")
      );
      for (TimelineObjectHolder<String, DataSegment> objectHolder : timelineObjectHolders) {
        for (PartitionChunk<DataSegment> partitionChunk : objectHolder.getObject()) {
          segments.add(partitionChunk.getObject());
        }
      }

      if (segments.isEmpty()) {
        log.warn("No segments found in the database!");
        return false;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              Batch batch = handle.createBatch();

              for (DataSegment segment : segments) {
                batch.add(
                    StringUtils.format(
                        "UPDATE %s SET used=true WHERE id = '%s'",
                        getSegmentsTable(),
                        segment.getIdentifier()
                    )
                );
              }
              batch.execute();

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, "Exception enabling datasource %s", ds);
      return false;
    }

    return true;
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
              handle.createStatement(
                  StringUtils.format("UPDATE %s SET used=true WHERE id = :id", getSegmentsTable())
              )
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
  public boolean removeDatasource(final String ds)
  {
    try {
      final int removed = connector.getDBI().withHandle(
          handle -> handle.createStatement(
              StringUtils.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", getSegmentsTable())
          ).bind("dataSource", ds).execute()
      );

      dataSourcesRef.get().remove(ds);

      if (removed == 0) {
        return false;
      }
    }
    catch (Exception e) {
      log.error(e, "Error removing datasource %s", ds);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeSegment(String ds, final String segmentID)
  {
    try {
      final int removed = connector.getDBI().withHandle(
          handle -> handle.createStatement(
              StringUtils.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable())
          ).bind("segmentID", segmentID).execute()
      );

      ConcurrentHashMap<String, DruidDataSource> dataSourceMap = dataSourcesRef.get();

      DruidDataSource dataSource = dataSourceMap.get(ds);
      if (dataSource != null) {
        dataSource.removePartition(segmentID);

        if (dataSource.isEmpty()) {
          dataSourceMap.remove(ds);
        }
      }

      if (removed == 0) {
        return false;
      }
    }
    catch (Exception e) {
      log.error(e, e.toString());
      return false;
    }

    return true;
  }

  @Override
  public boolean isStarted()
  {
    // isStarted() is synchronized together with start(), stop() and poll() to ensure that the latest currentStartOrder
    // is always visible. readLock should be used to avoid unexpected performance degradation of DruidCoordinator.
    readLock.lock();
    try {
      return currentStartOrder >= 0;
    }
    finally {
      readLock.unlock();
    }
  }

  @Override
  @Nullable
  public ImmutableDruidDataSource getInventoryValue(String key)
  {
    final DruidDataSource dataSource = dataSourcesRef.get().get(key);
    return dataSource == null ? null : dataSource.toImmutableDruidDataSource();
  }

  @Override
  public Collection<ImmutableDruidDataSource> getInventory()
  {
    return dataSourcesRef.get()
                         .values()
                         .stream()
                         .map(DruidDataSource::toImmutableDruidDataSource)
                         .collect(Collectors.toList());
  }

  @Override
  public Collection<String> getAllDatasourceNames()
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
    try {
      ConcurrentHashMap<String, DruidDataSource> newDataSources = new ConcurrentHashMap<>();

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
                        public DataSegment map(int index, ResultSet r, StatementContext ctx)
                            throws SQLException
                        {
                          try {
                            return replaceWithExistingSegmentIfPresent(
                                jsonMapper.readValue(r.getBytes("payload"), DataSegment.class)
                            );
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

      final Collection<DataSegment> segmentsFinal = Collections2.filter(
          segments, Predicates.notNull()
      );

      log.info("Polled and found %,d segments in the database", segments.size());

      for (final DataSegment segment : segmentsFinal) {
        String datasourceName = segment.getDataSource();

        DruidDataSource dataSource = newDataSources.get(datasourceName);
        if (dataSource == null) {
          dataSource = new DruidDataSource(
              datasourceName,
              ImmutableMap.of("created", DateTimes.nowUtc().toString())
          );

          Object shouldBeNull = newDataSources.put(
              datasourceName,
              dataSource
          );
          if (shouldBeNull != null) {
            log.warn(
                "Just put key[%s] into dataSources and what was there wasn't null!?  It was[%s]",
                datasourceName,
                shouldBeNull
            );
          }
        }

        // For performance reasons, make sure we check for the existence of a segment using containsSegment(),
        // which performs a key-based lookup, instead of calling contains() on the collection returned by
        // dataSource.getSegments(). In Map values collections, the contains() method is a linear scan.
        if (!dataSource.containsSegment(segment)) {
          dataSource.addSegment(segment);
        }
      }

      ConcurrentHashMap<String, DruidDataSource> current;
      do {
        current = dataSourcesRef.get();
      } while (!dataSourcesRef.compareAndSet(current, newDataSources));
    }
    catch (Exception e) {
      log.makeAlert(e, "Problem polling DB.").emit();
    }
  }

  /**
   * For the garbage collector in Java, it's better to keep new objects short-living, but once they are old enough
   * (i. e. promoted to old generation), try to keep them alive. In {@link #poll()}, we fetch and deserialize all
   * existing segments each time, and then replace them in {@link #dataSourcesRef}. This method allows to use already
   * existing (old) segments when possible, effectively interning them a-la {@link String#intern} or {@link
   * com.google.common.collect.Interner}, aiming to make the majority of {@link DataSegment} objects garbage soon after
   * they are deserialized and to die in young generation. It allows to avoid fragmentation of the old generation and
   * full GCs.
   */
  private DataSegment replaceWithExistingSegmentIfPresent(DataSegment segment)
  {
    DruidDataSource dataSource = dataSourcesRef.get().get(segment.getDataSource());
    if (dataSource == null) {
      return segment;
    }
    DataSegment alreadyExistingSegment = dataSource.getSegment(segment.getIdentifier());
    return alreadyExistingSegment != null ? alreadyExistingSegment : segment;
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
                throw Throwables.propagate(e);
              }
            }
            return result;
          }
        }
    );
  }
}
