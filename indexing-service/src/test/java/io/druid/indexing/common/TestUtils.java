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

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;

import io.druid.guice.ServerModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 */
public class TestUtils
{
  private final ObjectMapper jsonMapper;
  private final IndexMerger indexMerger;
  private final IndexMergerV9 indexMergerV9;
  private final IndexIO indexIO;

  public TestUtils()
  {
    jsonMapper = new DefaultObjectMapper();
    indexIO = new IndexIO(
        jsonMapper,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    indexMerger = new IndexMerger(jsonMapper, indexIO);
    indexMergerV9 = new IndexMergerV9(jsonMapper, indexIO);

    final List<? extends Module> list = new ServerModule().getJacksonModules();
    for (Module module : list) {
      jsonMapper.registerModule(module);
    }

    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(IndexIO.class, indexIO)
            .addValue(IndexMerger.class, indexMerger)
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
    );
  }

  public ObjectMapper getTestObjectMapper()
  {
    return jsonMapper;
  }

  public IndexMerger getTestIndexMerger()
  {
    return indexMerger;
  }

  public IndexMergerV9 getTestIndexMergerV9() {
    return indexMergerV9;
  }

  public IndexIO getTestIndexIO()
  {
    return indexIO;
  }

  public static boolean conditionValid(IndexingServiceCondition condition)
  {
    return conditionValid(condition, 1000);
  }

  public static boolean conditionValid(IndexingServiceCondition condition, long timeout)
  {
    try {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      stopwatch.start();
      while (!condition.isValid()) {
        Thread.sleep(100);
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
          throw new ISE("Cannot find running task");
        }
      }
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }
}
