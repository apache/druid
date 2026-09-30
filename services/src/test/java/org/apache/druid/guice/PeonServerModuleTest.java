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

package org.apache.druid.guice;

import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PeonServerModuleTest
{
  private static final Period DELAY = Period.seconds(30);

  @Test
  public void testNonQueryableTaskIgnoresUnannounceDelay()
  {
    final ServerConfig config = new ServerConfig().withUnannouncePropagationDelay(DELAY);
    final ServerConfig adjusted = PeonServerModule.adjustServerConfig(config, NoopTask.forDatasource("wiki"));

    Assertions.assertEquals(Period.ZERO, adjusted.getUnannouncePropagationDelay());
    Assertions.assertEquals(config.withUnannouncePropagationDelay(Period.ZERO), adjusted);
  }

  @Test
  public void testQueryableTaskKeepsUnannounceDelay()
  {
    final ServerConfig config = new ServerConfig().withUnannouncePropagationDelay(DELAY);
    final NoopTask queryableTask = new NoopTask(null, null, "wiki", 0, 0, null)
    {
      @Override
      public <T> QueryRunner<T> getQueryRunner(Query<T> query)
      {
        return new NoopQueryRunner<>();
      }
    };

    Assertions.assertSame(config, PeonServerModule.adjustServerConfig(config, queryableTask));
  }

  @Test
  public void testNullDatasourceTaskIgnoresUnannounceDelay()
  {
    final ServerConfig config = new ServerConfig().withUnannouncePropagationDelay(DELAY);
    final ServerConfig adjusted = PeonServerModule.adjustServerConfig(config, NoopTask.create());
    Assertions.assertEquals(Period.ZERO, adjusted.getUnannouncePropagationDelay());
  }

  @Test
  public void testZeroDelayIsUnchanged()
  {
    final ServerConfig config = new ServerConfig();
    Assertions.assertSame(config, PeonServerModule.adjustServerConfig(config, NoopTask.forDatasource("wiki")));
  }

  @Test
  public void testWithUnannouncePropagationDelayPreservesQueryQueuing()
  {
    final ServerConfig config = new ServerConfig(false).withUnannouncePropagationDelay(DELAY);
    Assertions.assertFalse(config.isEnableQueryRequestsQueuing());
    Assertions.assertEquals(DELAY, config.getUnannouncePropagationDelay());
  }
}
