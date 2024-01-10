package org.apache.druid.emitter.prometheus.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.emitter.prometheus.PrometheusEmitterConfig;

import java.util.SortedSet;

@JsonTypeName(MetricType.DimensionMapNames.GAUGE)
public class Gauge extends Metric<io.prometheus.client.Gauge> {
    public Gauge(
            @JsonProperty("dimensions") SortedSet<String> dimensions,
            @JsonProperty("type") MetricType type,
            @JsonProperty("help") String help
    ) {
        super(dimensions, type, help);
    }

    @Override
    public void record(String[] labelValues, double value) {
       this.getCollector().labels(labelValues).set(value);
    }

    @Override
    public void createCollector(String name, PrometheusEmitterConfig emitterConfig) {
        super.configure(name, emitterConfig);
        this.setCollector(
                new io.prometheus.client.Gauge.Builder()
                        .namespace(this.getNamespace())
                        .name(this.getFormattedName())
                        .labelNames(this.getDimensions())
                        .help(help)
                        .register()
        );
    }
}
