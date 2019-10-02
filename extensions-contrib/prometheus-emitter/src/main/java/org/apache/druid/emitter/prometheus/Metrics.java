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
