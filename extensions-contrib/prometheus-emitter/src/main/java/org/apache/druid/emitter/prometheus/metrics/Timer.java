package org.apache.druid.emitter.prometheus.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;
import org.apache.druid.emitter.prometheus.PrometheusEmitterConfig;
import org.apache.druid.emitter.prometheus.metrics.Histogram;
import org.apache.druid.emitter.prometheus.metrics.MetricType;

import java.util.SortedSet;

@Deprecated
@JsonTypeName(MetricType.DimensionMapNames.TIMER)
public class Timer extends Histogram {
    @Getter
    private final double conversionFactor;
    private static final double[] buckets = {.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300};

    public Timer(
            @JsonProperty("dimensions") SortedSet<String> dimensions,
            @JsonProperty("type") MetricType type,
            @JsonProperty("help") String help,
            @JsonProperty("conversionFactor") double conversionFactor
    ) {
        super(dimensions, type, help, buckets);
        this.conversionFactor = conversionFactor;
    }

    @Override
    public void record(String[] labelValues, double value) {
        this.getCollector().labels(labelValues).observe(value / this.conversionFactor);
    }
}
