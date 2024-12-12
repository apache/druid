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
import com.google.inject.Inject;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.skife.jdbi.v2.IDBI;

/**
 */
public class SQLMetadataRuleManagerProvider implements MetadataRuleManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final MetadataRuleManagerConfig config;
  private final MetadataStorageTablesConfig dbTables;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;
  private final IDBI dbi;
  private final AuditManager auditManager;

  @Inject
  public SQLMetadataRuleManagerProvider(
      ObjectMapper jsonMapper,
      MetadataRuleManagerConfig config,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      Lifecycle lifecycle,
      AuditManager auditManager
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
            public void start()
            {
              connector.createRulesTable();
              SQLMetadataRuleManager.createDefaultRule(
                  dbi,
                  dbTables.getRulesTable(),
                  config.getDefaultRule(),
                  jsonMapper
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
      throw new RuntimeException(e);
    }

    return new SQLMetadataRuleManager(jsonMapper, config, dbTables, connector, auditManager);
  }
}
