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

package io.druid.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.client.DruidDataSource;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class DatabaseSegmentManager
{
  private static final Logger log = new Logger(DatabaseSegmentManager.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final Supplier<DatabaseSegmentManagerConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final AtomicReference<ConcurrentHashMap<String, DruidDataSource>> dataSources;
  private final IDBI dbi;

  private volatile ScheduledExecutorService exec;

  private volatile boolean started = false;

  @Inject
  public DatabaseSegmentManager(
      ObjectMapper jsonMapper,
      Supplier<DatabaseSegmentManagerConfig> config,
      Supplier<DbTablesConfig> dbTables,
      IDBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dataSources = new AtomicReference<ConcurrentHashMap<String, DruidDataSource>>(
        new ConcurrentHashMap<String, DruidDataSource>()
    );
    this.dbi = dbi;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      this.exec = Execs.scheduledSingleThreaded("DatabaseSegmentManager-Exec--%d");

      final Duration delay = config.get().getPollDuration().toStandardDuration();
      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          delay,
          new Runnable()
          {
            @Override
            public void run()
            {
              poll();
            }
          }
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      started = false;
      dataSources.set(new ConcurrentHashMap<String, DruidDataSource>());
      exec.shutdownNow();
      exec = null;
    }
  }

  public boolean enableDatasource(final String ds)
  {
    try {
      VersionedIntervalTimeline<String, DataSegment> segmentTimeline = dbi.withHandle(
          new HandleCallback<VersionedIntervalTimeline<String, DataSegment>>()
          {
            @Override
            public VersionedIntervalTimeline<String, DataSegment> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT payload FROM %s WHERE dataSource = :dataSource", getSegmentsTable())
              )
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
                                     DataSegment segment = jsonMapper.readValue(
                                         payload,
                                         DataSegment.class
                                     );

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
      for (TimelineObjectHolder<String, DataSegment> objectHolder : segmentTimeline.lookup(
          new Interval(
              "0000-01-01/3000-01-01"
          )
      )) {
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
                    String.format(
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

  public boolean enableSegment(final String segmentId)
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format("UPDATE %s SET used=true WHERE id = :id", getSegmentsTable())
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

  public boolean removeDatasource(final String ds)
  {
    try {
      ConcurrentHashMap<String, DruidDataSource> dataSourceMap = dataSources.get();

      if (!dataSourceMap.containsKey(ds)) {
        log.warn("Cannot delete datasource %s, does not exist", ds);
        return false;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", getSegmentsTable())
              )
                    .bind("dataSource", ds)
                    .execute();

              return null;
            }
          }
      );

      dataSourceMap.remove(ds);
    }
    catch (Exception e) {
      log.error(e, "Error removing datasource %s", ds);
      return false;
    }

    return true;
  }

  public boolean removeSegment(String ds, final String segmentID)
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable())
              ).bind("segmentID", segmentID)
                    .execute();

              return null;
            }
          }
      );

      ConcurrentHashMap<String, DruidDataSource> dataSourceMap = dataSources.get();

      if (!dataSourceMap.containsKey(ds)) {
        log.warn("Cannot find datasource %s", ds);
        return false;
      }

      DruidDataSource dataSource = dataSourceMap.get(ds);
      dataSource.removePartition(segmentID);

      if (dataSource.isEmpty()) {
        dataSourceMap.remove(ds);
      }
    }
    catch (Exception e) {
      log.error(e, e.toString());
      return false;
    }

    return true;
  }

  public boolean isStarted()
  {
    return started;
  }

  public DruidDataSource getInventoryValue(String key)
  {
    return dataSources.get().get(key);
  }

  public Collection<DruidDataSource> getInventory()
  {
    return dataSources.get().values();
  }

  public Collection<String> getAllDatasourceNames()
  {
    synchronized (lock) {
      return dbi.withHandle(
          new HandleCallback<List<String>>()
          {
            @Override
            public List<String> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT DISTINCT(datasource) FROM %s", getSegmentsTable())
              )
                           .fold(
                               Lists.<String>newArrayList(),
                               new Folder3<ArrayList<String>, Map<String, Object>>()
                               {
                                 @Override
                                 public ArrayList<String> fold(
                                     ArrayList<String> druidDataSources,
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
                           );

            }
          }
      );
    }
  }

  public void poll()
  {
    try {
      if (!started) {
        return;
      }

      ConcurrentHashMap<String, DruidDataSource> newDataSources = new ConcurrentHashMap<String, DruidDataSource>();

      List<byte[]> payloadRows = dbi.withHandle(
          new HandleCallback<List<byte[]>>()
          {
            @Override
            public List<byte[]> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT payload FROM %s WHERE used=true", getSegmentsTable())
              ).map(ByteArrayMapper.FIRST).list();
            }
          }
      );

      if (payloadRows == null || payloadRows.isEmpty()) {
        log.warn("No segments found in the database!");
        return;
      }

      log.info("Polled and found %,d segments in the database", payloadRows.size());

      for (byte[] payload : payloadRows) {
        DataSegment segment = jsonMapper.readValue(payload, DataSegment.class);

        String datasourceName = segment.getDataSource();

        DruidDataSource dataSource = newDataSources.get(datasourceName);
        if (dataSource == null) {
          dataSource = new DruidDataSource(
              datasourceName,
              ImmutableMap.of("created", new DateTime().toString())
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

        if (!dataSource.getSegments().contains(segment)) {
          dataSource.addSegment(segment.getIdentifier(), segment);
        }
      }

      synchronized (lock) {
        if (started) {
          dataSources.set(newDataSources);
        }
      }
    }
    catch (Exception e) {
      log.error(e, "Problem polling DB.");
    }
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }
}
