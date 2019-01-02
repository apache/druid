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

package org.apache.druid.extensions.watermarking.storage.sql;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.druid.extensions.watermarking.WatermarkKeeperConfig;
import org.apache.druid.extensions.watermarking.storage.WatermarkStore;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.util.LongMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SqlWatermarkStore extends SQLMetadataConnector implements WatermarkStore
{
  private static final Logger log = new Logger(SqlWatermarkStore.class);
  final String tableName = "watermarking_timeline_metadata";
  final Supplier<SqlWatermarkStoreConfig> config;
  final WatermarkKeeperConfig keeperConfig;

  public SqlWatermarkStore(
      Supplier<SqlWatermarkStoreConfig> connectorConfigSupplier,
      WatermarkKeeperConfig keeperConfig
  )
  {
    super(connectorConfigSupplier::get, null);
    this.config = connectorConfigSupplier;
    this.keeperConfig = keeperConfig;
  }

  @Override
  public void update(String datasource, String type, DateTime timestamp)
  {
    getDBI().inTransaction(
        (handle, transactionStatus) -> {
          log.debug("Updating %s timeline watermark %s to %s", datasource, type, timestamp.toString());
          Long ts =
              handle
                  .createQuery(
                      StringUtils.format(
                          "SELECT timestamp FROM %1$s WHERE datasource = :ds AND type = :t ORDER BY timestamp DESC",
                          tableName
                      )
                  )
                  .bind("ds", datasource)
                  .bind("t", type)
                  .map(LongMapper.FIRST)
                  .first();

          DateTime dt = DateTimes.utc(ts);

          if (ts == null || dt.isBefore(timestamp)) {
            handle
                .createStatement(
                    StringUtils.format(
                        "INSERT INTO %1$s (insert_timestamp, timestamp, datasource, type) VALUES (:it, :ts, :ds, :t)",
                        tableName
                    )
                )
                .bind("it", DateTimes.nowUtc().getMillis())
                .bind("ts", timestamp.getMillis())
                .bind("ds", datasource)
                .bind("t", type)
                .execute();
            log.debug("Updated %s timeline watermark %s to %s", datasource, type, timestamp);
          }
          return null;
        }
    );
  }

  @Override
  public void rollback(String datasource, String type, DateTime timestamp)
  {
    getDBI().inTransaction(
        (handle, transactionStatus) -> {
          log.debug("Rolling back %s timeline watermark %s to %s", datasource, type, timestamp.toString());

          handle
              .createStatement(
                  StringUtils.format(
                      "DELETE FROM %1$s WHERE datasource = :ds AND type = :t AND timestamp > :ts",
                      tableName
                  )
              )
              .bind("ds", datasource)
              .bind("t", type)
              .bind("ts", timestamp)
              .execute();
          log.debug("Rolled back %s timeline watermark %s to %s", datasource, type, timestamp);
          return null;
        }
    );
  }

  @Override
  public void purgeHistory(String datasource, String type, DateTime timestamp)
  {
    getDBI().inTransaction(
        (handle, transactionStatus) -> {
          log.debug(
              "Purging history for %s timeline watermark %s older than %s",
              datasource,
              type,
              timestamp.toString()
          );

          handle
              .createStatement(
                  StringUtils.format(
                      "DELETE FROM %1$s WHERE datasource = :ds AND type = :t AND insert_timestamp < :ts",
                      tableName
                  )
              )
              .bind("ds", datasource)
              .bind("t", type)
              .bind("ts", timestamp)
              .execute();
          log.debug("Purged history for %s timeline watermark %s older than %s", datasource, type, timestamp);
          return null;
        }
    );
  }

  @Override
  public DateTime getValue(String datasource, String type)
  {
    return inReadOnlyTransaction((handle, status) -> {
      log.debug("Fetching %s timeline watermark for %s", datasource, type);

      String query = StringUtils.format(
          "SELECT timestamp FROM %1$s WHERE datasource = :ds AND type = :t ORDER BY timestamp DESC",
          tableName
      );

      Long millis = handle
          .createQuery(query)
          .setFetchSize(getStreamingFetchSize())
          .bind("ds", datasource)
          .bind("t", type)
          .map(LongMapper.FIRST)
          .first();

      DateTime dt = DateTimes.utc(millis);
      log.debug("Retrieved %s timeline watermark for %s of %s", datasource, type, dt.toString());
      return dt;
    });
  }

  @Override
  public Collection<String> getDatasources()
  {
    return inReadOnlyTransaction(
        (handle, status) -> {
          log.debug("Fetching datasources");

          final String query = StringUtils.format(
              "SELECT datasource FROM %1$s GROUP BY datasource",
              tableName
          );

          final Set<String> results =
              handle.createQuery(
                  query
              ).setFetchSize(
                  getStreamingFetchSize()
              ).map(
                  (index, r, ctx) -> r.getString("datasource")
              ).fold(
                  new HashSet<>(),
                  (datasources, s, foldController, statementContext) -> {
                    datasources.add(s);
                    return datasources;
                  }
              );
          log.debug("Retrieved datasources");
          return results;
        });
  }

  @Override
  public Map<String, DateTime> getValues(String datasource)
  {
    return inReadOnlyTransaction(
        (handle, status) -> {
          log.debug("Fetching %s timeline watermarks", datasource);

          String query = StringUtils.format(
              "SELECT type, MAX(timestamp) as timestamp FROM %1$s WHERE datasource = :ds GROUP BY type",
              tableName
          );

          Map<String, DateTime> results =
              handle.createQuery(query)
                    .setFetchSize(getStreamingFetchSize())
                    .bind("ds", datasource)
                    .fold(
                        new HashMap<>(),
                        (Folder3<Map<String, DateTime>, Map<String, Object>>)
                            (watermarks, stringObjectMap, foldController, statementContext) -> {
                              watermarks.put(
                                  MapUtils.getString(stringObjectMap, "type"),
                                  DateTimes.utc(MapUtils.getLong(
                                      stringObjectMap,
                                      "timestamp",
                                      0L
                                  ))
                              );
                              return watermarks;
                            }
                    );
          log.debug("Retrieved %s timeline watermarks.", datasource);
          return results;
        });
  }

  @Override
  public List<Pair<DateTime, DateTime>> getValueHistory(String datasource, String type, Interval range)
  {
    return inReadOnlyTransaction(
        (handle, status) -> {
          log.debug("Fetching %s timeline watermark history for %s between %s and %s",
                    datasource, type, range.getStart().toString(), range.getEnd().toString()
          );
          String query = StringUtils.format(
              "SELECT TOP :l timestamp, insert_timestamp FROM %1$s " +
              "WHERE datasource = :ds AND type = :t AND timestamp >= :start AND timestamp < :end " +
              "ORDER BY timestamp DESC",
              tableName
          );

          List<Pair<DateTime, DateTime>> history =
              handle.createQuery(query)
                    .setFetchSize(getStreamingFetchSize())
                    .bind("l", keeperConfig.getMaxHistoryResults())
                    .bind("ds", datasource)
                    .bind("t", type)
                    .bind("start", range.getStart().getMillis())
                    .bind("end", range.getEnd().getMillis())
                    .fold(
                        new ArrayList<>(),
                        (Folder3<List<Pair<DateTime, DateTime>>, Map<String, Object>>)
                            (timeline, stringObjectMap, foldController, statementContext) -> {
                              timeline.add(new Pair<>(
                                  DateTimes.utc(MapUtils.getLong(stringObjectMap, "timestamp", 0L)),
                                  DateTimes.utc(MapUtils.getLong(stringObjectMap, "insert_timestamp", 0L))
                              ));
                              return timeline;
                            }
                    );

          log.debug("Retrieved %s timeline watermark history for %s between %s and %s",
                    datasource, type, range.getStart().toString(), range.getEnd().toString()
          );
          return history;
        });
  }

  @Override
  public void initialize()
  {
    if (config.get().isCreateTimelineTables()) {
      log.info("Initializing sql timeline watermark metadata storage.");
      createTable(
          tableName,
          ImmutableList.of(
              StringUtils.format(
                  "CREATE TABLE %1$s (\n"
                  + "  id %2$s NOT NULL,\n"
                  + "  timestamp BIGINT NOT NULL,\n"
                  + "  insert_timestamp BIGINT NOT NULL,\n"
                  + "  datasource VARCHAR(255) NOT NULL,\n"
                  + "  type VARCHAR(255) NOT NULL,\n"
                  + "  PRIMARY KEY (id)\n"
                  + ")",
                  tableName, getSerialType()
              ),
              StringUtils.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName),
              StringUtils.format("CREATE INDEX idx_%1$s_timestamp ON %1$s(timestamp DESC)", tableName),
              StringUtils.format("CREATE INDEX idx_%1$s_insert_timestamp ON %1$s(insert_timestamp DESC)", tableName),
              StringUtils.format("CREATE INDEX idx_%1$s_type ON %1$s(type)", tableName)
          )
      );
      log.info("Initialized sql timeline watermark metadata storage.");
    }
  }
}
