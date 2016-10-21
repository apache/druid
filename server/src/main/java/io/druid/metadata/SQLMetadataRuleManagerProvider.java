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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import io.druid.audit.AuditManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.server.audit.SQLAuditManager;
import org.skife.jdbi.v2.IDBI;

/**
 */
public class SQLMetadataRuleManagerProvider implements MetadataRuleManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataRuleManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;
  private final IDBI dbi;
  private final AuditManager auditManager;

  @Inject
  public SQLMetadataRuleManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<MetadataRuleManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector,
      Lifecycle lifecycle,
      SQLAuditManager auditManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.connector = connector;
    this.dbi = connector.getDBI();
    this.lifecycle = lifecycle;
    this.auditManager = auditManager;
  }

  @Override
  public SQLMetadataRuleManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              connector.createRulesTable();
              SQLMetadataRuleManager.createDefaultRule(
                  dbi, dbTables.get().getRulesTable(), config.get().getDefaultRule(), jsonMapper
              );
            }

            @Override
            public void stop()
            {

            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new SQLMetadataRuleManager(jsonMapper, config, dbTables, connector, auditManager);
  }
}
