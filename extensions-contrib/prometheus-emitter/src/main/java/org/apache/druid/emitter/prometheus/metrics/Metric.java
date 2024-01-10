package org.apache.druid.emitter.prometheus.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.prometheus.client.SimpleCollector;
import lombok.Getter;
import lombok.Setter;
import org.apache.druid.emitter.prometheus.PrometheusEmitterConfig;
import org.apache.druid.java.util.common.StringUtils;

import java.util.SortedSet;
import java.util.regex.Pattern;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Gauge.class, name = MetricType.DimensionMapNames.GAUGE),
        @JsonSubTypes.Type(value = Histogram.class, name = MetricType.DimensionMapNames.HISTOGRAM),
        @JsonSubTypes.Type(value = Timer.class, name = MetricType.DimensionMapNames.TIMER),
        @JsonSubTypes.Type(value = Counter.class, name = MetricType.DimensionMapNames.COUNTER),
        @JsonSubTypes.Type(value = Summary.class, name = MetricType.DimensionMapNames.SUMMARY)
})
public abstract class Metric<T extends SimpleCollector<?>> {
    public static final Pattern PATTERN = Pattern.compile("[^a-zA-Z_:][^a-zA-Z0-9_:]*");
    private static final String TAG_HOSTNAME = "host_name";
    private static final String TAG_SERVICE = "druid_service";

    public final SortedSet<String> dimensions;
    public final MetricType type;
    public final String help;

    @Getter
    private String namespace;
    @Getter
    private String formattedName;

    @Getter @Setter @JsonIgnore
    private T collector;
    public Metric(SortedSet<String> dimensions,
                  MetricType type,
                  String help) {
        this.dimensions =  dimensions;
        this.type = type;
        this.help = help;
    }

    public abstract void record(String[] labelValues, double value);
    public abstract void createCollector(String name, PrometheusEmitterConfig emitterConfig);
    void configure(String name, PrometheusEmitterConfig emitterConfig) {
        if (emitterConfig.isAddHostAsLabel()) {
            this.dimensions.add(TAG_HOSTNAME);
        }
        if (emitterConfig.isAddServiceAsLabel()) {
            this.dimensions.add(TAG_SERVICE);
        }
        this.dimensions.addAll(emitterConfig.getExtraLabels().keySet());
        this.formattedName = PATTERN.matcher(StringUtils.toLowerCase(name)).replaceAll("_");
        this.namespace = emitterConfig.getNamespace();
    }

    public String[] getDimensions() {
        return this.dimensions.toArray(new String[0]);
    }
}
