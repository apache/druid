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
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@ManageLifecycle
public class SQLAuditManager implements AuditManager
{
  private final IDBI dbi;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ServiceEmitter emitter;
  private final ObjectMapper jsonMapper;
  private final SQLAuditManagerConfig config;

  @Inject
  public SQLAuditManager(
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables,
      ServiceEmitter emitter,
      @Json ObjectMapper jsonMapper,
      SQLAuditManagerConfig config
  )
  {
    this.dbi = connector.getDBI();
    this.dbTables = dbTables;
    this.emitter = emitter;
    this.jsonMapper = jsonMapper;
    this.config = config;
  }

  public String getAuditTable()
  {
    return dbTables.get().getAuditTable();
  }

  @Override
  public void doAudit(final AuditEntry auditEntry)
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            doAudit(auditEntry, handle);
            return null;
          }
        }
    );
  }

  @Override
  public void doAudit(AuditEntry auditEntry, Handle handle) throws IOException
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension("key", auditEntry.getKey())
            .setDimension("type", auditEntry.getType())
            .setDimension("author", auditEntry.getAuditInfo().getAuthor())
            .setDimension("comment", auditEntry.getAuditInfo().getComment())
            .setDimension("remote_address", auditEntry.getAuditInfo().getIp())
            .setDimension("created_date", auditEntry.getAuditTime().toString())
            .setDimension("payload", auditEntry.getPayload())
            .build("config/audit", 1)
    );

    handle.createStatement(
        StringUtils.format(
            "INSERT INTO %s ( audit_key, type, author, comment, created_date, payload) VALUES (:audit_key, :type, :author, :comment, :created_date, :payload)",
            getAuditTable()
        )
    )
          .bind("audit_key", auditEntry.getKey())
          .bind("type", auditEntry.getType())
          .bind("author", auditEntry.getAuditInfo().getAuthor())
          .bind("comment", auditEntry.getAuditInfo().getComment())
          .bind("created_date", auditEntry.getAuditTime().toString())
          .bind("payload", jsonMapper.writeValueAsBytes(auditEntry))
          .execute();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(final String key, final String type, Interval interval)
  {
    final Interval theInterval = getIntervalOrDefault(interval);
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
            .map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes("payload"), AuditEntry.class))
            .list()
    );
  }

  private Interval getIntervalOrDefault(Interval interval)
  {
    final Interval theInterval;
    if (interval == null) {
      DateTime now = DateTimes.nowUtc();
      theInterval = new Interval(now.minus(config.getAuditHistoryMillis()), now);
    } else {
      theInterval = interval;
    }
    return theInterval;
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
    final Interval theInterval = getIntervalOrDefault(interval);
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
            .map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes("payload"), AuditEntry.class))
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
              .map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes("payload"), AuditEntry.class))
              .list();
        }
    );
  }

}
