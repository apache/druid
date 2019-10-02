package org.apache.druid.emitter.prometheus;

import java.util.HashMap;
import java.util.Map;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;

public class Metrics {

    private Map<String, Metric> map = new HashMap<>();

    public Metric getByName(String name) {
        return map.get(name);
    }

    public Metrics(String namespace) {

        map.put("query/time", new Metric(
                new String[] { "dataSource", "type" },
                new Histogram.Builder()
                        .namespace(namespace)
                        .name("query_time")
                        .labelNames("dataSource", "type")
                        .buckets(new double[] { .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300 })
                        .register()));
        map.put("query/bytes",
                new Metric(new String[] { "dataSource", "type" },
                        new Counter.Builder()
                                .namespace(namespace)
                                .name("query_bytes_total")
                                .labelNames("dataSource", "type")
                                .register()));
        map.put("query/node/time",
                new Metric(new String[] { "server" },
                        new Histogram.Builder()
                                .namespace(namespace)
                                .name("query_node_time")
                                .labelNames("server")
                                .buckets(new double[] { .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300 })
                                .register()));
        map.put("query/node/ttfb",
                new Metric(new String[] { "server" },
                        new Histogram.Builder()
                                .namespace(namespace)
                                .name("query_node_ttfb_time")
                                .labelNames("server")
                                .buckets(new double[] { .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300 })
                                .register()));
        map.put("query/node/bytes",
                new Metric(new String[] { "server" },
                        new Counter.Builder()
                                .namespace(namespace)
                                .name("query_node_bytes_total")
                                .labelNames("server")
                                .register()));
    }


    public static class Metric {
        private final String[] dimensions;
        private final SimpleCollector collector;

        Metric(String[] dimensions, SimpleCollector collector) {
            this.dimensions = dimensions;
            this.collector = collector;
        }

        public String[] getDimensions() {
            return dimensions;
        }

        public SimpleCollector getCollector() {
            return collector;
        }
    }

}
