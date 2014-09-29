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

package io.druid.cli.validate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.metamx.common.UOE;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.segment.realtime.Schema;

import java.io.File;

/**
 */
@Command(
    name = "validator",
    description = "Validates that a given Druid JSON object is correctly formatted"
)
public class DruidJsonValidator implements Runnable
{
  @Option(name = "-f", title = "file", description = "file to validate", required = true)
  public String jsonFile;

  @Option(name = "-t", title = "type", description = "the type of schema to validate", required = true)
  public String type;

  @Override
  public void run()
  {
    File file = new File(jsonFile);
    if (!file.exists()) {
      System.out.printf("File[%s] does not exist.%n", file);
    }

    final ObjectMapper jsonMapper = new DefaultObjectMapper();

    try {
      if (type.equalsIgnoreCase("query")) {
        jsonMapper.readValue(file, Query.class);
      } else if (type.equalsIgnoreCase("hadoopConfig")) {
        jsonMapper.readValue(file, HadoopDruidIndexerConfig.class);
      } else if (type.equalsIgnoreCase("task")) {
        jsonMapper.readValue(file, Task.class);
      } else if (type.equalsIgnoreCase("realtimeSchema")) {
        jsonMapper.readValue(file, Schema.class);
      } else {
        throw new UOE("Unknown type[%s]", type);
      }
    }
    catch (Exception e) {
      System.out.println("INVALID JSON!");
      throw Throwables.propagate(e);
    }
  }
}
