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

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import org.apache.calcite.runtime.Hook;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;

/**
 * JUnit Rule that adds a Calcite hook to log and remember Druid queries.
 */
public class QueryLogHook implements TestRule
{
  private static final Logger log = new Logger(QueryLogHook.class);

  private final ObjectMapper objectMapper;
  private final List<Query> recordedQueries = Lists.newCopyOnWriteArrayList();

  public QueryLogHook(final ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public static QueryLogHook create()
  {
    return new QueryLogHook(new DefaultObjectMapper());
  }

  public static QueryLogHook create(final ObjectMapper objectMapper)
  {
    return new QueryLogHook(objectMapper);
  }

  public void clearRecordedQueries()
  {
    recordedQueries.clear();
  }

  public List<Query> getRecordedQueries()
  {
    return ImmutableList.copyOf(recordedQueries);
  }

  @Override
  public Statement apply(final Statement base, final Description description)
  {
    return new Statement()
    {
      @Override
      public void evaluate() throws Throwable
      {
        clearRecordedQueries();

        final Function<Object, Object> function = new Function<Object, Object>()
        {
          @Override
          public Object apply(final Object query)
          {
            try {
              recordedQueries.add((Query) query);
              log.info(
                  "Issued query: %s",
                  objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query)
              );
            }
            catch (Exception e) {
              log.warn(e, "Failed to serialize query: %s", query);
            }
            return null;
          }
        };

        try (final Hook.Closeable unhook = Hook.QUERY_PLAN.add(function)) {
          base.evaluate();
        }
      }
    };
  }
}
