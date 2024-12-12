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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.runtime.Hook;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import java.util.List;

/**
 * Class to log Druid queries.
 */
public class QueryLogHook
{
  private static final Logger log = new Logger(QueryLogHook.class);

  private final ObjectMapper objectMapper;
  private final List<Query<?>> recordedQueries = Lists.newCopyOnWriteArrayList();

  public QueryLogHook(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public List<Query<?>> getRecordedQueries()
  {
    return ImmutableList.copyOf(recordedQueries);
  }

  protected void accept(Object query)
  {
    try {
      recordedQueries.add((Query<?>) query);
      log.info(
          "Issued query: %s",
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query)
      );
    }
    catch (Exception e) {
      log.warn(e, "Failed to serialize query: %s", query);
    }
  }

  public void logQueriesFor(Runnable r)
  {
    try (final Hook.Closeable unhook = Hook.QUERY_PLAN.addThread(this::accept)) {
      r.run();
    }
  }

  public void logQueriesForGlobal(Runnable r)
  {
    try (final Hook.Closeable unhook = Hook.QUERY_PLAN.add(this::accept)) {
      r.run();
    }
  }
}
