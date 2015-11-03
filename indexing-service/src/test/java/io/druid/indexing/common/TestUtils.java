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

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.metamx.common.ISE;
import io.druid.guice.ServerModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.IndexMerger;
import io.druid.segment.column.ColumnConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 */
public class TestUtils
{
  private final ObjectMapper jsonMapper;
  private final IndexMerger indexMerger;
  private final IndexMaker indexMaker;
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
    indexMaker = new IndexMaker(jsonMapper, indexIO);

    final List<? extends Module> list = new ServerModule().getJacksonModules();
    for (Module module : list) {
      jsonMapper.registerModule(module);
    }

    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(IndexIO.class, indexIO)
            .addValue(IndexMerger.class, indexMerger)
            .addValue(IndexMaker.class, indexMaker)
            .addValue(ObjectMapper.class, jsonMapper)
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

  public IndexMaker getTestIndexMaker()
  {
    return indexMaker;
  }

  public IndexIO getTestIndexIO()
  {
    return indexIO;
  }

  public static boolean conditionValid(IndexingServiceCondition condition)
  {
    try {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      stopwatch.start();
      while (!condition.isValid()) {
        Thread.sleep(100);
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
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
