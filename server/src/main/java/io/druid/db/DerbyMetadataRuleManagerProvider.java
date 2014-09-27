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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.Lifecycle;
import org.skife.jdbi.v2.IDBI;

/**
 */
public class DerbyMetadataRuleManagerProvider implements MetadataRuleManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataRuleManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final MetadataStorageConnector dbConnector;
  private final Lifecycle lifecycle;
  private final IDBI dbi;

  @Inject
  public DerbyMetadataRuleManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<MetadataRuleManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      MetadataStorageConnector dbConnector,
      IDBI dbi,
      Lifecycle lifecycle
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbConnector = dbConnector;
    this.dbi = dbi;
    this.lifecycle = lifecycle;
  }

  @Override
  public DerbyMetadataRuleManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              dbConnector.createRulesTable();
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

    return new DerbyMetadataRuleManager(jsonMapper, config, dbTables, dbi);
  }
}