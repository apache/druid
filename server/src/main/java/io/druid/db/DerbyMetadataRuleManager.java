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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.server.coordinator.rules.Rule;
import org.skife.jdbi.v2.IDBI;

import java.sql.Blob;
import java.util.List;
import java.util.Map;

public class DerbyMetadataRuleManager extends SQLMetadataRuleManager
{
  private final ObjectMapper jsonMapper;

  @Inject
  public DerbyMetadataRuleManager(
      @Json ObjectMapper jsonMapper,
      Supplier<MetadataRuleManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      IDBI dbi
  ) {
    super(jsonMapper, config, dbTables, dbi);
    this.jsonMapper = jsonMapper;
  }

  @Override
  protected List<Rule> getRules(Map<String, Object> stringObjectMap) {
    try {
      Blob payload = (Blob)stringObjectMap.get("payload");
      List<Rule> rules = jsonMapper.readValue(
          payload.getBinaryStream(), new TypeReference<List<Rule>>() {}
      );
      return rules;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
