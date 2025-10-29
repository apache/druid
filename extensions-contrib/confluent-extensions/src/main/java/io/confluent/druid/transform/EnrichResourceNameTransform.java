/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class EnrichResourceNameTransform implements Transform
{
  private final String name;
  private final String metricNameDimension;
  private final Set<String> kafkaMetricPrefixes;
  private final String kafkaResourceIdDimension;
  private final String kafkaResourceIdDerivedDimension;
  private final Set<String> tableflowMetricPrefixes;
  private final String tableflowResourceIdDimension;
  private final Set<String> connectMetricPrefixes;
  private final String connectResourceIdDimension;
  private final Set<String> ksqlMetricPrefixes;
  private final String ksqlResourceIdDimension;
  private final Set<String> schemaRegistryMetricPrefixes;
  private final String schemaRegistryResourceIdDimension;
  private final Set<String> fcpMetricPrefixes;
  private final String fcpResourceIdDimension;
  private final String lookupName;
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  public EnrichResourceNameTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("metricNameDimension") final String metricNameDimension,
      @JsonProperty("kafkaMetricPrefixes") final Set<String> kafkaMetricPrefixes,
      @JsonProperty("kafkaResourceIdDimension") final String kafkaResourceIdDimension,
      @JsonProperty("kafkaResourceIdDerivedDimension") final String kafkaResourceIdDerivedDimension,
      @JsonProperty("tableflowMetricPrefixes") final Set<String> tableflowMetricPrefixes,
      @JsonProperty("tableflowResourceIdDimension") final String tableflowResourceIdDimension,
      @JsonProperty("connectMetricPrefixes") final Set<String> connectMetricPrefixes,
      @JsonProperty("connectResourceIdDimension") final String connectResourceIdDimension,
      @JsonProperty("ksqlMetricPrefixes") final Set<String> ksqlMetricPrefixes,
      @JsonProperty("ksqlResourceIdDimension") final String ksqlResourceIdDimension,
      @JsonProperty("schemaRegistryMetricPrefixes") final Set<String> schemaRegistryMetricPrefixes,
      @JsonProperty("schemaRegistryResourceIdDimension") final String schemaRegistryResourceIdDimension,
      @JsonProperty("fcpMetricPrefixes") final Set<String> fcpMetricPrefixes,
      @JsonProperty("fcpResourceIdDimension") final String fcpResourceIdDimension,
      @JsonProperty("lookupName") final String lookupName,
      @JacksonInject LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    this.name = Preconditions.checkNotNull(name, "Specify output-column name");
    this.metricNameDimension = Preconditions.checkNotNull(metricNameDimension, "Specify metric-name column : metricNameDimension");
    this.kafkaMetricPrefixes = kafkaMetricPrefixes != null ? kafkaMetricPrefixes : new HashSet<>();
    this.kafkaResourceIdDimension = Preconditions.checkNotNull(kafkaResourceIdDimension, "Specify kafka-id column : kafkaResourceIdDimension");
    this.kafkaResourceIdDerivedDimension = Preconditions.checkNotNull(kafkaResourceIdDerivedDimension, "Specify parent kafka-id column : kafkaResourceIdDerivedDimension");
    this.tableflowMetricPrefixes = tableflowMetricPrefixes != null ? tableflowMetricPrefixes : new HashSet<>();
    this.tableflowResourceIdDimension = Preconditions.checkNotNull(tableflowResourceIdDimension, "Specify kafka-id column for tableflow metrics: tableflowResourceIdDimension");
    this.connectMetricPrefixes = connectMetricPrefixes != null ? connectMetricPrefixes : new HashSet<>();
    this.connectResourceIdDimension = Preconditions.checkNotNull(connectResourceIdDimension, "Specify connector-id column : connectResourceIdDimension");
    this.ksqlMetricPrefixes = ksqlMetricPrefixes != null ? ksqlMetricPrefixes : new HashSet<>();
    this.ksqlResourceIdDimension = Preconditions.checkNotNull(ksqlResourceIdDimension, "Specify ksql-id column : ksqlResourceIdDimension");
    this.schemaRegistryMetricPrefixes = schemaRegistryMetricPrefixes != null ? schemaRegistryMetricPrefixes : new HashSet<>();
    this.schemaRegistryResourceIdDimension = Preconditions.checkNotNull(schemaRegistryResourceIdDimension, "Specify sr-id column : schemaRegistryResourceIdDimension");
    this.fcpMetricPrefixes = fcpMetricPrefixes != null ? fcpMetricPrefixes : new HashSet<>();
    this.fcpResourceIdDimension = Preconditions.checkNotNull(fcpResourceIdDimension, "Specify fcp-id column : fcpResourceIdDimension");
    this.lookupName = Preconditions.checkNotNull(lookupName, "Specify lookup-name");
    this.lookupProvider = Preconditions.checkNotNull(lookupProvider, "Specify lookupProvider");
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getMetricNameDimension()
  {
    return metricNameDimension;
  }

  @JsonProperty
  public String getKafkaResourceIdDimension()
  {
    return kafkaResourceIdDimension;
  }

  @JsonProperty
  public String getKafkaResourceIdDerivedDimension()
  {
    return kafkaResourceIdDerivedDimension;
  }

  @JsonProperty
  public String getTableflowResourceIdDimension()
  {
    return tableflowResourceIdDimension;
  }

  @JsonProperty
  public String getConnectResourceIdDimension()
  {
    return connectResourceIdDimension;
  }

  @JsonProperty
  public String getKsqlResourceIdDimension()
  {
    return ksqlResourceIdDimension;
  }

  @JsonProperty
  public String getSchemaRegistryResourceIdDimension()
  {
    return schemaRegistryResourceIdDimension;
  }

  @JsonProperty
  public String getFcpResourceIdDimension()
  {
    return fcpResourceIdDimension;
  }

  @JsonProperty
  public String getLookupName()
  {
    return lookupName;
  }

  @JsonProperty
  public Set<String> getKafkaMetricPrefixes()
  {
    return kafkaMetricPrefixes;
  }

  @JsonProperty
  public Set<String> getTableflowMetricPrefixes()
  {
    return tableflowMetricPrefixes;
  }

  @JsonProperty
  public Set<String> getConnectMetricPrefixes()
  {
    return connectMetricPrefixes;
  }

  @JsonProperty
  public Set<String> getKsqlMetricPrefixes()
  {
    return ksqlMetricPrefixes;
  }

  @JsonProperty
  public Set<String> getSchemaRegistryMetricPrefixes()
  {
    return schemaRegistryMetricPrefixes;
  }

  @JsonProperty
  public Set<String> getFcpMetricPrefixes()
  {
    return fcpMetricPrefixes;
  }

  @Override
  public RowFunction getRowFunction()
  {
    return row -> {
      Optional<LookupExtractorFactoryContainer> container = lookupProvider.get(lookupName);
      if (!container.isPresent()) {
        return null;
      }
      LookupExtractor lookup = container.get().getLookupExtractorFactory().get();
      String metricName = row.getRaw(metricNameDimension).toString();

      if (metricName != null) {
        // Check if metric name starts with any kafka prefix
        for (String prefix : kafkaMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, kafkaResourceIdDimension, kafkaResourceIdDerivedDimension);
          }
        }
        // Check if metric name starts with any connect prefix
        for (String prefix : connectMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, connectResourceIdDimension, null);
          }
        }
        // Check if metric name starts with any ksql prefix
        for (String prefix : ksqlMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, ksqlResourceIdDimension, null);
          }
        }
        // Check if metric name starts with any schema registry prefix
        for (String prefix : schemaRegistryMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, schemaRegistryResourceIdDimension, null);
          }
        }
        // Check if metric name starts with any flink-compute-pool prefix
        for (String prefix : fcpMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, fcpResourceIdDimension, null);
          }
        }
        // Check if metric name starts with any tableflow prefix
        for (String prefix : tableflowMetricPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, tableflowResourceIdDimension, null);
          }
        }
      }

      return null;
    };
  }

  private String enrichNameFromLookup(Row row, LookupExtractor lookup, String resourceIdDimension, String resourceIdDerivedDimension)
  {
    Object resourceIdObject = row.getRaw(resourceIdDimension);

    String resourceId = resourceIdObject != null ? resourceIdObject.toString() : null;
    if (resourceId == null) {
      if (resourceIdDerivedDimension != null) {
        Object resourceIdDerivedObject = row.getRaw(resourceIdDerivedDimension);
        if (resourceIdDerivedObject != null) {
          resourceId = TenantUtils.extractTenant(resourceIdDerivedObject.toString());
        }
      }
      if (resourceId == null) {
        return null;
      }
    }

    return lookup.apply(resourceId);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    Set<String> columns = new HashSet<>();
    columns.add(this.name);
    columns.add(this.metricNameDimension);
    columns.add(this.kafkaResourceIdDimension);
    columns.add(this.tableflowResourceIdDimension);
    columns.add(this.connectResourceIdDimension);
    columns.add(this.ksqlResourceIdDimension);
    columns.add(this.schemaRegistryResourceIdDimension);
    columns.add(this.fcpResourceIdDimension);
    return columns;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EnrichResourceNameTransform)) {
      return false;
    }
    EnrichResourceNameTransform that = (EnrichResourceNameTransform) o;
    return name.equals(that.name) &&
      Objects.equals(kafkaMetricPrefixes, that.kafkaMetricPrefixes) &&
      Objects.equals(tableflowMetricPrefixes, that.tableflowMetricPrefixes) &&
      Objects.equals(connectMetricPrefixes, that.connectMetricPrefixes) &&
      Objects.equals(ksqlMetricPrefixes, that.ksqlMetricPrefixes) &&
      Objects.equals(schemaRegistryMetricPrefixes, that.schemaRegistryMetricPrefixes) &&
      Objects.equals(fcpMetricPrefixes, that.fcpMetricPrefixes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, kafkaMetricPrefixes, tableflowMetricPrefixes, connectMetricPrefixes,
            ksqlMetricPrefixes, schemaRegistryMetricPrefixes, fcpMetricPrefixes);
  }

  @Override
  public String toString()
  {
    return "EnrichResourceNameTransform{" +
      "name='" + name + '\'' +
      ", kafkaMetricPrefixes=" + kafkaMetricPrefixes +
      ", tableflowMetricPrefixes=" + tableflowMetricPrefixes +
      ", connectMetricPrefixes=" + connectMetricPrefixes +
      ", ksqlMetricPrefixes=" + ksqlMetricPrefixes +
      ", schemaRegistryMetricPrefixes=" + schemaRegistryMetricPrefixes +
      ", fcpMetricPrefixes=" + fcpMetricPrefixes +
      '}';
  }
}
