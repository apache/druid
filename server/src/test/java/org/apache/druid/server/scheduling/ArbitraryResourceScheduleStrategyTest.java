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

package org.apache.druid.server.scheduling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.common.exception.AllowedRegexErrorResponseTransformStrategy;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.QueryLaningStrategy;
import org.apache.druid.server.QuerySchedulerConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.HttpMethod;
import java.nio.ByteBuffer;
import java.util.List;

public class ArbitraryResourceScheduleStrategyTest
{
  private BlockingPool<ByteBuffer> pool;

  @Before
  public void setup()
  {
    ServerConfig defaultConfig = new ServerConfig();
    ServerConfig serverConfig = new ServerConfig(
        200,
        1000,
        defaultConfig.isEnableRequestLimit(),
        defaultConfig.getMaxIdleTime(),
        defaultConfig.getDefaultQueryTimeout(),
        defaultConfig.getMaxScatterGatherBytes(),
        defaultConfig.getMaxSubqueryRows(),
        defaultConfig.getMaxQueryTimeout(),
        defaultConfig.getMaxRequestHeaderSize(),
        defaultConfig.getGracefulShutdownTimeout(),
        defaultConfig.getUnannouncePropagationDelay(),
        defaultConfig.getInflateBufferSize(),
        defaultConfig.getCompressionLevel(),
        true,
        ImmutableList.of(HttpMethod.OPTIONS),
        true,
        new AllowedRegexErrorResponseTransformStrategy(ImmutableList.of(".*"))
    );
    QuerySchedulerConfig querySchedulerConfig = new QuerySchedulerConfig()
    {
      @Override
      public QueryLaningStrategy getLaningStrategy()
      {
        return new ManualQueryLaningStrategy(ImmutableMap.of("lane1", 50), true);
      }
    };
    DruidProcessingConfig processingConfig = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public int getNumMergeBuffersConfigured()
      {
        return 4;
      }
    };
    pool = querySchedulerConfig.getMergeBufferStrategy()
                               .getMergeBufferPool(serverConfig, querySchedulerConfig, processingConfig);
  }

  @Test
  public void testTotalCapacity()
  {
    Assert.assertEquals(4, pool.maxSize());
  }

  @Test
  public void testNotExistLane()
  {
    List<ReferenceCountingResourceHolder<ByteBuffer>> holders = pool.takeBatch("not exist", 1);
    Assert.assertEquals(1, holders.size());
    holders.forEach(ReferenceCountingResourceHolder::close);
  }

  @Test
  public void testDefaultLane()
  {
    List<ReferenceCountingResourceHolder<ByteBuffer>> holders = pool.takeBatch(null, 1);
    Assert.assertEquals(1, holders.size());
    holders.forEach(ReferenceCountingResourceHolder::close);
  }
}
