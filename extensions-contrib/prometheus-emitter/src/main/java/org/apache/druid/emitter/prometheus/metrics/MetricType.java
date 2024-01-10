package org.apache.druid.emitter.prometheus.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum MetricType {
    count(DimensionMapNames.COUNTER),
    gauge(DimensionMapNames.GAUGE),
    timer(DimensionMapNames.TIMER),
    histogram(DimensionMapNames.HISTOGRAM),
    summary(DimensionMapNames.SUMMARY);

    @Getter
    private final String dimensionMapName;

    // TODO: get rid of this inner class after upgrading jackson in parent
    //       ref: https://github.com/FasterXML/jackson-databind/issues/2739
    static class DimensionMapNames {
        static final String GAUGE = "gauge";
        static final String COUNTER = "count";
        static final String TIMER = "timer";
        static final String HISTOGRAM = "histogram";
        static final String SUMMARY = "summary";
    }
}
