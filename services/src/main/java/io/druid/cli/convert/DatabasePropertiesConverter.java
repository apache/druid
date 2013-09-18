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

package io.druid.cli.convert;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class DatabasePropertiesConverter implements PropertyConverter
{

  private final List<String> tableProperties = Lists.newArrayList(
      "druid.database.segmentTable",
      "druid.database.configTable",
      "druid.database.ruleTable",
      "druid.database.taskLockTable",
      "druid.database.taskLogTable",
      "druid.database.taskTable"
  );
  private AtomicBoolean ran = new AtomicBoolean(false);

  @Override
  public boolean canHandle(String property)
  {
    return tableProperties.contains(property) && !ran.get();
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    if (!ran.getAndSet(true)) {
      String tablePrefix = properties.getProperty("druid.database.segmentTable");

      if (tablePrefix == null) {
        tablePrefix = "druid";
      }
      else {
        tablePrefix = tablePrefix.split("_")[0];
      }

      Map<String, String> retVal = Maps.newLinkedHashMap();

      retVal.put("druid.db.tables.base", tablePrefix);

      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.segmentTable", "segments");
      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.configTable", "config");
      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.ruleTable", "rules");
      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.taskTable", "tasks");
      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.taskLockTable", "taskLock");
      addIfNotDefault(properties, tablePrefix, retVal, "druid.database.taskLogTable", "taskLog");

      return retVal;
    }
    return ImmutableMap.of();
  }

  private void addIfNotDefault(
      Properties properties,
      String tablePrefix,
      Map<String, String> retVal,
      String property,
      String tablename
  )
  {
    final String value = properties.getProperty(property);
    if (value != null) {
      if (!value.equals(String.format("%s_%s", tablePrefix, tablename))) {
        retVal.put(String.format("druid.db.tables.%s", tablename), value);
      }
    }
  }
}
