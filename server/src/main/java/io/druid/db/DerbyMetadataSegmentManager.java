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
import com.metamx.common.logger.Logger;
import io.druid.client.DruidDataSource;
import io.druid.guice.ManageLifecycle;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class DerbyMetadataSegmentManager extends SQLMetadataSegmentManager
{
  private static final Logger log = new Logger(DerbyMetadataSegmentManager.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final AtomicReference<ConcurrentHashMap<String, DruidDataSource>> dataSources;
  private final IDBI dbi;

  @Inject
  public DerbyMetadataSegmentManager(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      IDBI dbi
  )
  {
    super(jsonMapper, config, dbTables, dbi);
    this.jsonMapper = jsonMapper;
    this.dbTables = dbTables;
    this.dataSources = new AtomicReference<ConcurrentHashMap<String, DruidDataSource>>(
        new ConcurrentHashMap<String, DruidDataSource>()
    );
    this.dbi = dbi;
  }

  @Override
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
                           .fold(
                               new VersionedIntervalTimeline<String, DataSegment>(Ordering.natural()),
                               new Folder3<VersionedIntervalTimeline<String, DataSegment>, Map<String, Object>>()
                               {
                                 @Override
                                 public VersionedIntervalTimeline<String, DataSegment> fold(
                                     VersionedIntervalTimeline<String, DataSegment> timeline,
                                     Map<String, Object> stringObjectMap,
                                     FoldController foldController,
                                     StatementContext statementContext
                                 ) throws SQLException
                                 {
                                   try {
                                     java.sql.Clob payloadClob = (java.sql.Clob)stringObjectMap.get("payload");
                                     String payload = payloadClob.getSubString(1, (int)payloadClob.length()).replace("\\", "");
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

  @Override
  public void poll()
  {
    try {
      if (!isStarted()) {
        return;
      }

      ConcurrentHashMap<String, DruidDataSource> newDataSources = new ConcurrentHashMap<String, DruidDataSource>();

      List<Map<String, Object>> segmentRows = dbi.withHandle(
          new HandleCallback<List<Map<String, Object>>>()
          {
            @Override
            public List<Map<String, Object>> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT * FROM %s WHERE used=true", getSegmentsTable())
              ).fold(
                  new LinkedList<Map<String, Object>>(),
                  new Folder3<LinkedList<Map<String, Object>>, Map<String, Object>>()
                  {
                    @Override
                    public LinkedList<Map<String, Object>> fold(
                        LinkedList<Map<String, Object>> retVal,
                        Map<String, Object> stringObjectMap,
                        FoldController foldController,
                        StatementContext statementContext
                    ) throws SQLException
                    {
                      java.sql.Clob payloadClob = (java.sql.Clob)stringObjectMap.get("payload");
                      String payload = payloadClob.getSubString(1, (int)payloadClob.length()).replace("\\", "");
                      stringObjectMap.put("payload", payload);
                      retVal.add(stringObjectMap);
                      return retVal;
                    }
                  }
              );
            }
          }
      );



      if (segmentRows == null || segmentRows.isEmpty()) {
        log.warn("No segments found in the database!");
        return;
      }

      log.info("Polled and found %,d segments in the database", segmentRows.size());

      for (final Map<String, Object> segmentRow : segmentRows) {
        DataSegment segment = jsonMapper.readValue((String) segmentRow.get("payload"), DataSegment.class);

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
        if (isStarted()) {
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
