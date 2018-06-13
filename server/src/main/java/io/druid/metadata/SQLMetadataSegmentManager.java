/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.client.DruidDataSource;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
@ManageLifecycle
public class SQLMetadataSegmentManager implements MetadataSegmentManager
{
  private static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataSegmentManager.class);

  // Use to synchronize start() and stop(). These methods should be synchronized to prevent from being called at the
  // same time if two different threads are calling them. This might be possible if a druid coordinator gets and drops
  // leadership repeatedly in quick succession.
  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final AtomicReference<ConcurrentHashMap<String, DruidDataSource>> dataSourcesRef;
  private final SQLMetadataConnector connector;

  private volatile ListeningScheduledExecutorService exec = null;
  private volatile ListenableFuture<?> future = null;
  private volatile boolean started;

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
    this.dataSourcesRef = new AtomicReference<>(
        new ConcurrentHashMap<String, DruidDataSource>()
    );
    this.connector = connector;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DatabaseSegmentManager-Exec--%d"));

      final Duration delay = config.get().getPollDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                poll();
              }
              catch (Exception e) {
                log.makeAlert(e, "uncaught exception in segment manager polling thread").emit();

              }
            }
          },
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      final ConcurrentHashMap<String, DruidDataSource> emptyMap = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, DruidDataSource> current;
      do {
        current = dataSourcesRef.get();
      } while (!dataSourcesRef.compareAndSet(current, emptyMap));

      future.cancel(false);
      future = null;
      exec.shutdownNow();
      exec = null;
      started = false;
    }
  }

  @Override
  public boolean enableDatasource(final String ds)
  {
    try {
      final IDBI dbi = connector.getDBI();
      VersionedIntervalTimeline<String, DataSegment> segmentTimeline = connector.inReadOnlyTransaction(
          new TransactionCallback<VersionedIntervalTimeline<String, DataSegment>>()
          {
            @Override
            public VersionedIntervalTimeline<String, DataSegment> inTransaction(
                Handle handle, TransactionStatus status
            ) throws Exception
            {
              return handle
                  .createQuery(StringUtils.format(
                      "SELECT payload FROM %s WHERE dataSource = :dataSource",
                      getSegmentsTable()
                  ))
                  .setFetchSize(connector.getStreamingFetchSize())
                  .bind("dataSource", ds)
                  .map(ByteArrayMapper.FIRST)
                  .fold(
                      new VersionedIntervalTimeline<String, DataSegment>(Ordering.natural()),
                      new Folder3<VersionedIntervalTimeline<String, DataSegment>, byte[]>()
                      {
                        @Override
                        public VersionedIntervalTimeline<String, DataSegment> fold(
                            VersionedIntervalTimeline<String, DataSegment> timeline,
                            byte[] payload,
                            FoldController foldController,
                            StatementContext statementContext
                        ) throws SQLException
                        {
                          try {
                            final DataSegment segment = DATA_SEGMENT_INTERNER.intern(jsonMapper.readValue(
                                payload,
                                DataSegment.class
                            ));

                            timeline.add(
                                segment.getInterval(),
                                segment.getVersion(),
                                segment.getShardSpec().createChunk(segment)
                            );

                            return timeline;
                          }
                          catch (Exception e) {
                            throw new SQLException(e.toString());
                          }
                        }
                      }
                  );
            }
          }
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
            public Void withHandle(Handle handle) throws Exception
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
            public Void withHandle(Handle handle) throws Exception
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
    return started;
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
                              ) throws SQLException
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
      if (!started) {
        return;
      }

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
            public List<DataSegment> inTransaction(Handle handle, TransactionStatus status) throws Exception
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
                            return DATA_SEGMENT_INTERNER.intern(jsonMapper.readValue(
                                r.getBytes("payload"),
                                DataSegment.class
                            ));
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
          public List<Interval> inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            Iterator<Interval> iter = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT start, %2$send%2$s FROM %1$s WHERE dataSource = :dataSource and start >= :start and %2$send%2$s <= :end and used = false ORDER BY start, %2$send%2$s",
                        getSegmentsTable(), connector.getQuoteString()
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
