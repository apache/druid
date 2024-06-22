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

package org.apache.druid.server.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@ManageLifecycle
public class SQLAuditManager implements AuditManager
{
  private final IDBI dbi;
  private final SQLMetadataConnector connector;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ServiceEmitter emitter;
  private final ObjectMapper jsonMapper;
  private final SQLAuditManagerConfig config;
  private final AuditSerdeHelper serdeHelper;

  private final ResultSetMapper<AuditEntry> resultMapper;

  @Inject
  public SQLAuditManager(
      AuditManagerConfig config,
      AuditSerdeHelper serdeHelper,
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables,
      ServiceEmitter emitter,
      @Json ObjectMapper jsonMapper
  )
  {
    this.dbi = connector.getDBI();
    this.connector = connector;
    this.dbTables = dbTables;
    this.emitter = emitter;
    this.jsonMapper = jsonMapper;
    this.serdeHelper = serdeHelper;
    this.resultMapper = new AuditEntryMapper();

    if (!(config instanceof SQLAuditManagerConfig)) {
      throw DruidException.defensive("Config[%s] is not an instance of SQLAuditManagerConfig", config);
    }
    this.config = (SQLAuditManagerConfig) config;
  }

  @LifecycleStart
  public void start()
  {
    connector.createAuditTable();
  }

  @LifecycleStop
  public void stop()
  {
    // Do nothing
  }

  private String getAuditTable()
  {
    return dbTables.get().getAuditTable();
  }

  @Override
  public void doAudit(AuditEntry event)
  {
    dbi.withHandle(
        handle -> {
          doAudit(event, handle);
          return 0;
        }
    );
  }

  private ServiceMetricEvent.Builder createMetricEventBuilder(AuditEntry entry)
  {
    ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
        .setDimension("key", entry.getKey())
        .setDimension("type", entry.getType())
        .setDimension("author", entry.getAuditInfo().getAuthor())
        .setDimension("comment", entry.getAuditInfo().getComment())
        .setDimension("remote_address", entry.getAuditInfo().getIp())
        .setDimension("created_date", entry.getAuditTime().toString());

    if (config.isIncludePayloadAsDimensionInMetric()) {
      builder.setDimension("payload", entry.getPayload().serialized());
    }

    return builder;
  }

  @Override
  public void doAudit(AuditEntry event, Handle handle) throws IOException
  {
    if (!serdeHelper.shouldProcessAuditEntry(event)) {
      return;
    }

    emitter.emit(createMetricEventBuilder(event).setMetric("config/audit", 1));

    final AuditEntry record = serdeHelper.processAuditEntry(event);
    handle.createStatement(
        StringUtils.format(
            "INSERT INTO %s (audit_key, type, author, comment, created_date, payload)"
            + " VALUES (:audit_key, :type, :author, :comment, :created_date, :payload)",
            getAuditTable()
        )
    )
          .bind("audit_key", record.getKey())
          .bind("type", record.getType())
          .bind("author", record.getAuditInfo().getAuthor())
          .bind("comment", record.getAuditInfo().getComment())
          .bind("created_date", record.getAuditTime().toString())
          .bind("payload", jsonMapper.writeValueAsBytes(record))
          .execute();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(final String key, final String type, Interval interval)
  {
    final Interval theInterval = createAuditHistoryIntervalIfNull(interval);
    return dbi.withHandle(
        (Handle handle) -> handle
            .createQuery(
                StringUtils.format(
                    "SELECT payload FROM %s WHERE audit_key = :audit_key and type = :type and "
                    + "created_date between :start_date and :end_date ORDER BY created_date",
                    getAuditTable()
                )
            )
            .bind("audit_key", key)
            .bind("type", type)
            .bind("start_date", theInterval.getStart().toString())
            .bind("end_date", theInterval.getEnd().toString())
            .map(resultMapper)
            .list()
    );
  }

  private Interval createAuditHistoryIntervalIfNull(Interval interval)
  {
    if (interval == null) {
      DateTime now = DateTimes.nowUtc();
      return new Interval(now.minus(config.getAuditHistoryMillis()), now);
    } else {
      return interval;
    }
  }

  private int getLimit(int limit) throws IllegalArgumentException
  {
    if (limit < 1) {
      throw new IllegalArgumentException("Limit must be greater than zero!");
    }
    return limit;
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(final String type, Interval interval)
  {
    final Interval theInterval = createAuditHistoryIntervalIfNull(interval);
    return dbi.withHandle(
        (Handle handle) -> handle
            .createQuery(
                StringUtils.format(
                    "SELECT payload FROM %s WHERE type = :type and created_date between :start_date and "
                    + ":end_date ORDER BY created_date",
                    getAuditTable()
                )
            )
            .bind("type", type)
            .bind("start_date", theInterval.getStart().toString())
            .bind("end_date", theInterval.getEnd().toString())
            .map(resultMapper)
            .list()
    );
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(final String key, final String type, int limit)
      throws IllegalArgumentException
  {
    return fetchAuditHistoryLastEntries(key, type, limit);
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(final String type, int limit)
      throws IllegalArgumentException
  {
    return fetchAuditHistoryLastEntries(null, type, limit);
  }

  @Override
  public int removeAuditLogsOlderThan(final long timestamp)
  {
    DateTime dateTime = DateTimes.utc(timestamp);
    return dbi.withHandle(
        handle -> {
          Update sql = handle.createStatement(
              StringUtils.format(
                  "DELETE FROM %s WHERE created_date < :date_time",
                  getAuditTable()
              )
          );
          return sql.bind("date_time", dateTime.toString())
                    .execute();
        }
    );
  }

  private List<AuditEntry> fetchAuditHistoryLastEntries(final String key, final String type, int limit)
      throws IllegalArgumentException
  {
    final int theLimit = getLimit(limit);
    String queryString = StringUtils.format("SELECT payload FROM %s WHERE type = :type", getAuditTable());
    if (key != null) {
      queryString += " and audit_key = :audit_key";
    }
    queryString += " ORDER BY created_date DESC";
    final String theQueryString = queryString;

    return dbi.withHandle(
        (Handle handle) -> {
          Query<Map<String, Object>> query = handle.createQuery(theQueryString);
          if (key != null) {
            query.bind("audit_key", key);
          }
          return query
              .bind("type", type)
              .setMaxRows(theLimit)
              .map(resultMapper)
              .list();
        }
    );
  }

  private class AuditEntryMapper implements ResultSetMapper<AuditEntry>
  {
    @Override
    public AuditEntry map(int index, ResultSet r, StatementContext ctx) throws SQLException
    {
      return JacksonUtils.readValue(jsonMapper, r.getBytes("payload"), AuditEntry.class);
    }
  }

}
