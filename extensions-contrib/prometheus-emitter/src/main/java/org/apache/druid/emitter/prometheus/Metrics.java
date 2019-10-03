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

package org.apache.druid.emitter.prometheus;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;

import java.util.HashMap;
import java.util.Map;

public class Metrics
{

  private Map<String, Metric> map = new HashMap<>();

  public Metric getByName(String name)
  {
    return map.get(name);
  }

  //TODO: revise metric types
  //TODO: revise Histogram bucket values
  public Metrics(String namespace)
  {

    map.put("query/time", new Metric(
        new String[]{"dataSource", "type"},
        new Histogram.Builder()
            .namespace(namespace)
            .name("query_time")
            .labelNames("dataSource", "type")
            .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
            .register()
    ));
    map.put(
        "query/bytes",
        new Metric(
            new String[]{"dataSource", "type"},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_bytes_total")
                .labelNames("dataSource", "type")
                .register()
        )
    );
    map.put(
        "query/node/time",
        new Metric(
            new String[]{"server"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_node_time")
                .labelNames("server")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/node/ttfb",
        new Metric(
            new String[]{"server"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_node_ttfb_time")
                .labelNames("server")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/node/bytes",
        new Metric(
            new String[]{"server"},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_node_bytes_total")
                .labelNames("server")
                .register()
        )
    );
    map.put(
        "query/node/backpressure",
        new Metric(
            new String[]{"server"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_node_backpressure_time")
                .labelNames("server")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/intervalChunk/time",
        new Metric(
            new String[]{},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_interval_chunk_time")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/segment/time",
        new Metric(
            new String[]{},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_segment_time")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/wait/time",
        new Metric(
            new String[]{},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_wait_time")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "segment/scan/pending",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("segment_scan_pending")
                .register()
        )
    );
    map.put(
        "query/segmentAndCache/time",
        new Metric(
            new String[]{},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_segment_and_cache_time")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/cpu/time",
        new Metric(
            new String[]{"dataSource", "type"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("query_cpu_time")
                .labelNames("dataSource", "type")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "query/count",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_count_total")
                .register()
        )
    );
    map.put(
        "query/success/count",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_success_count_total")
                .register()
        )
    );
    map.put(
        "query/failed/count",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_failed_count_total")
                .register()
        )
    );
    map.put(
        "query/interrupted/count",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_interrupted_count_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/numEntries",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_numentries_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/sizeBytes",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_sizebytes_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/hits",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_hits_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/misses",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_misses_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/evictions",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_evictions_total")
                .register()
        )
    );
    // Leaning toward ignoring this since it should be derived from delta/hits above
    //    "query/cache/delta/hitRate" : { "dimensions" : [], "type" : "count", "convertRange" : true },

    map.put(
        "query/cache/delta/averageBytes",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_averagebytes_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/timeouts",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_timeouts_total")
                .register()
        )
    );
    map.put(
        "query/cache/delta/errors",
        new Metric(
            new String[]{},
            new Counter.Builder()
                .namespace(namespace)
                .name("query_cache_delta_errors_total")
                .register()
        )
    );

    map.put(
        "query/cache/total/numentries",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_numentries")
                .register()
        )
    );
    map.put(
        "query/cache/total/sizeBytes",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_sizebytes")
                .register()
        )
    );
    map.put(
        "query/cache/total/hits",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_hits")
                .register()
        )
    );
    map.put(
        "query/cache/total/misses",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_misses")
                .register()
        )
    );
    map.put(
        "query/cache/total/evictions",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_evictions")
                .register()
        )
    );
    map.put(
        "query/cache/total/hitRate",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_hitrate")
                .register()
        )
    );
    map.put(
        "query/cache/total/averageBytes",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_averagebytes")
                .register()
        )
    );
    map.put(
        "query/cache/total/timeouts",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_timeouts")
                .register()
        )
    );
    map.put(
        "query/cache/total/errors",
        new Metric(
            new String[]{},
            new Gauge.Builder()
                .namespace(namespace)
                .name("query_cache_total_errors")
                .register()
        )
    );
    map.put(
        "ingest/events/thrownAway",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_events_thrownaway_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/events/unparseable",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_events_unparseable_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/events/duplicate",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_events_duplicate_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/events/processed",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_events_processed_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/events/messageGap",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_events_messagegap_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/rows/output",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_rows_output_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/persists/count",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_persists_count_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/persists/time",
        new Metric(
            new String[]{"dataSource"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("ingest_persists_time")
                .labelNames("dataSource")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "ingest/persists/cpu",
        new Metric(
            new String[]{"dataSource"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("ingest_persists_cpu_time")
                .labelNames("dataSource")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "ingest/persists/backPressure",
        new Metric(
            new String[]{"dataSource"},
            new Gauge.Builder()
                .namespace(namespace)
                .name("ingest_persists_backpressure")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/persists/failed",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_persists_failed_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/handoff/failed",
        new Metric(
            new String[]{"dataSource"},
            new Counter.Builder()
                .namespace(namespace)
                .name("ingest_handoff_failed_total")
                .labelNames("dataSource")
                .register()
        )
    );
    map.put(
        "ingest/merge/time",
        new Metric(
            new String[]{"dataSource"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("ingest_merge_time")
                .labelNames("dataSource")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "ingest/merge/cpu",
        new Metric(
            new String[]{"dataSource"},
            new Histogram.Builder()
                .namespace(namespace)
                .name("ingest_merge_cpu_time")
                .labelNames("dataSource")
                .buckets(new double[]{.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                .register()
        )
    );
    map.put(
        "ingest/kafka/lag",
        new Metric(
            new String[]{"dataSource"},
            new Gauge.Builder()
                .namespace(namespace)
                .name("ingest_kafka_lag")
                .labelNames("dataSource")
                .register()
        )
    );


//    "ingest/kafka/lag" : { "dimensions" : ["dataSource"], "type" : "gauge" },
//    "ingest/kafka/maxLag" : { "dimensions" : ["dataSource"], "type" : "gauge" },
//    "ingest/kafka/avgLag" : { "dimensions" : ["dataSource"], "type" : "gauge" },
//
//    "task/success/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//    "task/failed/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//    "task/running/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//    "task/pending/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//    "task/waiting/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//
//    "task/run/time" : { "dimensions" : ["dataSource", "taskType"], "type" : "timer" },
//    "segment/added/bytes" : { "dimensions" : ["dataSource", "taskType"], "type" : "count" },
//    "segment/moved/bytes" : { "dimensions" : ["dataSource", "taskType"], "type" : "count" },
//    "segment/nuked/bytes" : { "dimensions" : ["dataSource", "taskType"], "type" : "count" },
//
//    "segment/assigned/count" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/moved/count" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/dropped/count" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/deleted/count" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/unneeded/count" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/unavailable/count" : { "dimensions" : ["dataSource"], "type" : "count" },
//    "segment/underReplicated/count" : { "dimensions" : ["dataSource", "tier"], "type" : "count" },
//    "segment/cost/raw" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/cost/normalization" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/cost/normalized" : { "dimensions" : ["tier"], "type" : "count" },
//    "segment/loadQueue/size" : { "dimensions" : ["server"], "type" : "gauge" },
//    "segment/loadQueue/failed" : { "dimensions" : ["server"], "type" : "gauge" },
//    "segment/loadQueue/count" : { "dimensions" : ["server"], "type" : "gauge" },
//    "segment/dropQueue/count" : { "dimensions" : ["server"], "type" : "gauge" },
//    "segment/size" : { "dimensions" : ["dataSource"], "type" : "gauge" },
//    "segment/overShadowed/count" : { "dimensions" : [], "type" : "gauge" },
//
//    "segment/max" : { "dimensions" : [], "type" : "gauge"},
//    "segment/used" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" },
//    "segment/usedPercent" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge", "convertRange" : true },
//    "segment/pendingDelete" : { "dimensions" : [], "type" : "gauge"},
//
//    "jvm/pool/committed" : { "dimensions" : ["poolKind", "poolName"], "type" : "gauge" },
//    "jvm/pool/init" : { "dimensions" : ["poolKind", "poolName"], "type" : "gauge" },
//    "jvm/pool/max" : { "dimensions" : ["poolKind", "poolName"], "type" : "gauge" },
//    "jvm/pool/used" : { "dimensions" : ["poolKind", "poolName"], "type" : "gauge" },
//    "jvm/bufferpool/count" : { "dimensions" : ["bufferPoolName"], "type" : "gauge" },
//    "jvm/bufferpool/used" : { "dimensions" : ["bufferPoolName"], "type" : "gauge" },
//    "jvm/bufferpool/capacity" : { "dimensions" : ["bufferPoolName"], "type" : "gauge" },
//    "jvm/mem/init" : { "dimensions" : ["memKind"], "type" : "gauge" },
//    "jvm/mem/max" : { "dimensions" : ["memKind"], "type" : "gauge" },
//    "jvm/mem/used" : { "dimensions" : ["memKind"], "type" : "gauge" },
//    "jvm/mem/committed" : { "dimensions" : ["memKind"], "type" : "gauge" },
//    "jvm/gc/count" : { "dimensions" : ["gcName"], "type" : "count" },
//    "jvm/gc/cpu" : { "dimensions" : ["gcName"], "type" : "timer" },
//
//    "ingest/events/buffered" : { "dimensions" : ["serviceName, bufferCapacity"], "type" : "gauge"},
//
//    "sys/swap/free" : { "dimensions" : [], "type" : "gauge"},
//    "sys/swap/max" : { "dimensions" : [], "type" : "gauge"},
//    "sys/swap/pageIn" : { "dimensions" : [], "type" : "gauge"},
//    "sys/swap/pageOut" : { "dimensions" : [], "type" : "gauge"},
//    "sys/disk/write/count" : { "dimensions" : ["fsDevName"], "type" : "count"},
//    "sys/disk/read/count" : { "dimensions" : ["fsDevName"], "type" : "count"},
//    "sys/disk/write/size" : { "dimensions" : ["fsDevName"], "type" : "count"},
//    "sys/disk/read/size" : { "dimensions" : ["fsDevName"], "type" : "count"},
//    "sys/net/write/size" : { "dimensions" : [], "type" : "count"},
//    "sys/net/read/size" : { "dimensions" : [], "type" : "count"},
//    "sys/fs/used" : { "dimensions" : ["fsDevName", "fsDirName", "fsTypeName", "fsSysTypeName", "fsOptions"], "type" : "gauge"},
//    "sys/fs/max" : { "dimensions" : ["fsDevName", "fsDirName", "fsTypeName", "fsSysTypeName", "fsOptions"], "type" : "gauge"},
//    "sys/mem/used" : { "dimensions" : [], "type" : "gauge"},
//    "sys/mem/max" : { "dimensions" : [], "type" : "gauge"},
//    "sys/storage/used" : { "dimensions" : ["fsDirName"], "type" : "gauge"},
//    "sys/cpu" : { "dimensions" : ["cpuName", "cpuTime"], "type" : "gauge"},
//
//    "coordinator-segment/count" : { "dimensions" : ["dataSource"], "type" : "gauge" },
//    "historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" }

  }


  public static class Metric
  {
    private final String[] dimensions;
    private final SimpleCollector collector;

    Metric(String[] dimensions, SimpleCollector collector)
    {
      this.dimensions = dimensions;
      this.collector = collector;
    }

    public String[] getDimensions()
    {
      return dimensions;
    }

    public SimpleCollector getCollector()
    {
      return collector;
    }
  }

}
