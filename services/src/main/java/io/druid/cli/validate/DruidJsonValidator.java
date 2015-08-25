/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli.validate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.UOE;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;

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
