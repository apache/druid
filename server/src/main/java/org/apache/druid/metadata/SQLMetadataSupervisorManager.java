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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ManageLifecycle
public class SQLMetadataSupervisorManager implements MetadataSupervisorManager
{
  private static final Logger log = new Logger(SQLMetadataSupervisorManager.class);

  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final IDBI dbi;

  @Inject
  public SQLMetadataSupervisorManager(
      @Json ObjectMapper jsonMapper,
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    this.dbTables = dbTables;
    this.dbi = connector.getDBI();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    connector.createSupervisorsTable();
  }

  @Override
  public void insert(final String id, final SupervisorSpec spec)
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %s (spec_id, created_date, payload) VALUES (:spec_id, :created_date, :payload)",
                    getSupervisorsTable()
                )
            )
                  .bind("spec_id", id)
                  .bind("created_date", DateTimes.nowUtc().toString())
                  .bind("payload", jsonMapper.writeValueAsBytes(spec))
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public Map<String, List<VersionedSupervisorSpec>> getAll()
  {
    return ImmutableMap.copyOf(
        dbi.withHandle(
            new HandleCallback<Map<String, List<VersionedSupervisorSpec>>>()
            {
              @Override
              public Map<String, List<VersionedSupervisorSpec>> withHandle(Handle handle)
              {
                return handle.createQuery(
                    StringUtils.format(
                        "SELECT id, spec_id, created_date, payload FROM %1$s ORDER BY id DESC",
                        getSupervisorsTable()
                    )
                ).map(
                    new ResultSetMapper<Pair<String, VersionedSupervisorSpec>>()
                    {
                      @Override
                      public Pair<String, VersionedSupervisorSpec> map(int index, ResultSet r, StatementContext ctx)
                          throws SQLException
                      {
                        SupervisorSpec payload;
                        try {
                          payload = jsonMapper.readValue(
                              r.getBytes("payload"),
                              new TypeReference<SupervisorSpec>()
                              {
                              }
                          );
                        }
                        catch (JsonParseException | JsonMappingException e) {
                          log.warn("Failed to deserialize payload for spec_id[%s]", r.getString("spec_id"));
                          payload = null;
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }

                        return Pair.of(
                            r.getString("spec_id"),
                            new VersionedSupervisorSpec(payload, r.getString("created_date"))
                        );
                      }
                    }
                ).fold(
                    new HashMap<>(),
                    new Folder3<Map<String, List<VersionedSupervisorSpec>>, Pair<String, VersionedSupervisorSpec>>()
                    {
                      @Override
                      public Map<String, List<VersionedSupervisorSpec>> fold(
                          Map<String, List<VersionedSupervisorSpec>> retVal,
                          Pair<String, VersionedSupervisorSpec> pair,
                          FoldController foldController,
                          StatementContext statementContext
                      )
                      {
                        try {
                          String specId = pair.lhs;
                          retVal.computeIfAbsent(specId, sId -> new ArrayList<>()).add(pair.rhs);
                          return retVal;
                        }
                        catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                );
              }
            }
        )
    );
  }

  @Override
  public Map<String, SupervisorSpec> getLatest()
  {
    return ImmutableMap.copyOf(
        dbi.withHandle(
            new HandleCallback<Map<String, SupervisorSpec>>()
            {
              @Override
              public Map<String, SupervisorSpec> withHandle(Handle handle)
              {
                return handle.createQuery(
                    StringUtils.format(
                        "SELECT r.spec_id, r.payload "
                        + "FROM %1$s r "
                        + "INNER JOIN(SELECT spec_id, max(id) as id FROM %1$s GROUP BY spec_id) latest "
                        + "ON r.id = latest.id",
                        getSupervisorsTable()
                    )
                ).map(
                    new ResultSetMapper<Pair<String, SupervisorSpec>>()
                    {
                      @Override
                      public Pair<String, SupervisorSpec> map(int index, ResultSet r, StatementContext ctx)
                          throws SQLException
                      {
                        try {
                          return Pair.of(
                              r.getString("spec_id"),
                              jsonMapper.readValue(
                                  r.getBytes("payload"), new TypeReference<SupervisorSpec>()
                                  {
                                  }
                              )
                          );
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                ).fold(
                    new HashMap<>(),
                    new Folder3<Map<String, SupervisorSpec>, Pair<String, SupervisorSpec>>()
                    {
                      @Override
                      public Map<String, SupervisorSpec> fold(
                          Map<String, SupervisorSpec> retVal,
                          Pair<String, SupervisorSpec> stringObjectMap,
                          FoldController foldController,
                          StatementContext statementContext
                      )
                      {
                        try {
                          retVal.put(stringObjectMap.lhs, stringObjectMap.rhs);
                          return retVal;
                        }
                        catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                );
              }
            }
        )
    );
  }

  private String getSupervisorsTable()
  {
    return dbTables.get().getSupervisorTable();
  }
}
