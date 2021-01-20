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

package org.apache.druid.segment.incremental.oak;

import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.junit.Ignore;

/**
 * This class wraps Oak relevant benchmarks and adds additional static initialization
 * to add Oak to the available incremental indexes.
 */
@Ignore
public class OakBenchmarks
{
  public static void initModule()
  {
    IncrementalIndexCreator.JSON_MAPPER.registerModule(OakIncrementalIndexModule.JACKSON_MODULE);
  }

  public static class IncrementalIndexReadBenchmark extends org.apache.druid.benchmark.indexing.IncrementalIndexReadBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class IndexIngestionBenchmark extends org.apache.druid.benchmark.indexing.IndexIngestionBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class IndexPersistBenchmark extends org.apache.druid.benchmark.indexing.IndexPersistBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class GroupByBenchmark extends org.apache.druid.benchmark.query.GroupByBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class ScanBenchmark extends org.apache.druid.benchmark.query.ScanBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class SearchBenchmark extends org.apache.druid.benchmark.query.SearchBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class TimeseriesBenchmark extends org.apache.druid.benchmark.query.TimeseriesBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class TopNBenchmark extends org.apache.druid.benchmark.query.TopNBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }

  public static class FilteredAggregatorBenchmark extends org.apache.druid.benchmark.FilteredAggregatorBenchmark
  {
    static {
      OakBenchmarks.initModule();
    }
  }
}
