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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultiset;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.AdaptedBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.BaseTableProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.granularity.BaseGranularitySpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.transform.TransformSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;


/**
 *
 */
public class DataSchema
{
  private static final Logger log = new Logger(DataSchema.class);

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(DataSchema schema)
  {
    return new Builder(schema);
  }

  private final String dataSource;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;
  @Nullable
  private final SegmentGranularitySpec segmentGranularitySpec;
  private final TransformSpec transformSpec;
  private final TimestampSpec timestampSpec;
  @Nullable
  private final DimensionsSpec dimensionsSpec;
  @Nullable
  private final List<AggregateProjectionSpec> projections;
  @Nullable
  private final BaseTableProjectionSpec baseTable;
  private final BaseTableProjectionSpec effectiveBaseTableSpec;
  private final GranularitySpec effectiveGranularitySpec;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") @Nullable GranularitySpec granularitySpec,
      @JsonProperty("segmentGranularitySpec") @Nullable SegmentGranularitySpec segmentGranularitySpec,
      @JsonProperty("transformSpec") TransformSpec transformSpec,
      @JsonProperty("projections") @Nullable List<AggregateProjectionSpec> projections,
      @JsonProperty("baseTable") @Nullable BaseTableProjectionSpec baseTable,
      @Deprecated @JsonProperty("parser") @Nullable Map<String, Object> parserMap
  )
  {
    InvalidInput.conditionalException(parserMap == null, "parser was removed in Druid 37, define the timestampSpec and dimensionsSpec on the schema directly instead of nested inside the parser definition");
    validateDatasourceName(dataSource);
    this.dataSource = dataSource;
    this.transformSpec = transformSpec == null ? TransformSpec.NONE : transformSpec;
    this.projections = projections;
    this.baseTable = baseTable;
    this.timestampSpec = InvalidInput.notNull(timestampSpec, "timestampSpec");

    if (baseTable != null) {
      // BaseTable mode: the spec owns dimensions/metrics; segment granularity + intervals come from the optional
      // top-level segmentGranularitySpec. The legacy top-level schema fields (granularitySpec/dimensionsSpec/
      // metricsSpec) must be absent on the wire, reject loudly rather than silently picking one source of truth.
      rejectLegacyTopLevelSchemaFields(dimensionsSpec, aggregators, granularitySpec);
      this.dimensionsSpec = null;
      this.aggregators = new AggregatorFactory[0];
      this.granularitySpec = null;
      this.segmentGranularitySpec = segmentGranularitySpec;
      this.effectiveBaseTableSpec = baseTable;
    } else {
      // Legacy mode: existing v9-era field handling, unchanged. segmentGranularitySpec is a baseTable-only concept;
      // the legacy granularitySpec already carries segment granularity, so reject it here.
      if (segmentGranularitySpec != null) {
        throw InvalidInput.exception(
            "segmentGranularitySpec is only valid alongside 'baseTable'; in legacy mode use the top-level"
            + " granularitySpec instead"
        );
      }
      this.segmentGranularitySpec = null;
      this.aggregators = aggregators == null ? new AggregatorFactory[]{} : aggregators;
      this.dimensionsSpec = dimensionsSpec == null ? null : computeDimensionsSpec(
          timestampSpec,
          dimensionsSpec,
          this.aggregators
      );
      if (granularitySpec == null) {
        log.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
        this.granularitySpec = new UniformGranularitySpec(null, null, null);
      } else {
        this.granularitySpec = granularitySpec;
      }
      this.effectiveBaseTableSpec = new AdaptedBaseTableProjectionSpec(
          this.granularitySpec,
          this.dimensionsSpec,
          this.aggregators
      );
    }
    this.effectiveGranularitySpec = computeEffectiveGranularitySpec(
        this.effectiveBaseTableSpec,
        this.segmentGranularitySpec
    );

    // Fail-fast if there are output name collisions. Note: because of the pull-from-parser magic in getDimensionsSpec,
    // this validation is not necessarily going to be able to catch everything. It will run again in getDimensionsSpec.
    computeAndValidateOutputFieldNames(
        effectiveBaseTableSpec.getDimensionsSpec(),
        effectiveBaseTableSpec.getMetrics()
    );
    final GranularitySpec effectiveGranularity = this.effectiveGranularitySpec;
    validateProjections(
        this.projections,
        effectiveGranularity instanceof UniformGranularitySpec ? effectiveGranularity.getSegmentGranularity() : null
    );

    if (effectiveGranularity.isRollup() && effectiveBaseTableSpec.getMetrics().length == 0) {
      log.warn(
          "Rollup is enabled for dataSource [%s] but no metricsSpec has been provided. "
          + "Are you sure this is what you want?",
          dataSource
      );
    }
  }

  private static void rejectLegacyTopLevelSchemaFields(
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] aggregators,
      @Nullable GranularitySpec granularitySpec
  )
  {
    final List<String> conflicts = new ArrayList<>();
    if (dimensionsSpec != null) {
      conflicts.add("dimensionsSpec");
    }
    if (aggregators != null && aggregators.length > 0) {
      conflicts.add("metricsSpec");
    }
    if (granularitySpec != null) {
      conflicts.add("granularitySpec");
    }
    if (!conflicts.isEmpty()) {
      throw InvalidInput.exception(
          "When 'baseTable' is set, top-level field(s) %s must not also be set — declare them inside the baseTable spec instead",
          conflicts
      );
    }
  }

  /**
   * Reconstructs the full {@link GranularitySpec} that legacy consumers read through {@link #getGranularitySpec()}.
   * <p>
   * In legacy mode the effective spec is an {@link AdaptedBaseTableProjectionSpec} wrapping the top-level
   * {@link GranularitySpec}, which is already the complete source of truth (segment + query granularity + rollup), so
   * it is returned as-is. In baseTable mode the pieces live apart and are recombined: segment granularity + intervals
   * from {@code segmentGranularitySpec} (or defaults), query granularity from the spec's
   * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column (absent ⇒ {@code NONE}).
   */
  private static GranularitySpec computeEffectiveGranularitySpec(
      BaseTableProjectionSpec effectiveSpec,
      @Nullable SegmentGranularitySpec segmentGranularitySpec
  )
  {
    if (effectiveSpec instanceof AdaptedBaseTableProjectionSpec) {
      return ((AdaptedBaseTableProjectionSpec) effectiveSpec).getGranularitySpec();
    }
    final Granularity segmentGranularity = segmentGranularitySpec != null
                                           ? segmentGranularitySpec.getSegmentGranularity()
                                           : BaseGranularitySpec.DEFAULT_SEGMENT_GRANULARITY;
    final List<Interval> intervals = segmentGranularitySpec != null ? segmentGranularitySpec.getIntervals() : null;
    return new UniformGranularitySpec(
        segmentGranularity,
        queryGranularityFromSpec(effectiveSpec),
        false,
        intervals
    );
  }

  /**
   * Reads the query granularity a base-table spec carries as a {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}
   * virtual column, mirroring the read-side {@code ClusteredValueGroupsBaseTableSchema}. When the virtual column is
   * absent (or not a recognized granularity expression) the query granularity is {@code NONE}.
   */
  private static Granularity queryGranularityFromSpec(BaseTableProjectionSpec spec)
  {
    final VirtualColumn vc = spec.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    if (vc == null) {
      return Granularities.NONE;
    }
    final Granularity granularity = Granularities.fromVirtualColumn(vc);
    return granularity == null ? Granularities.NONE : granularity;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("timestampSpec")
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @Nullable
  @JsonIgnore
  public DimensionsSpec getDimensionsSpec()
  {
    return baseTable != null ? baseTable.getDimensionsSpec() : dimensionsSpec;
  }

  @JsonIgnore
  public AggregatorFactory[] getAggregators()
  {
    return baseTable != null ? baseTable.getMetrics() : aggregators;
  }

  /**
   * The full effective {@link GranularitySpec} for legacy consumers: in legacy mode the top-level granularitySpec; in
   * baseTable mode the recombination of {@link #getSegmentGranularitySpec()}, the spec's query-granularity virtual
   * column, and rollup. See {@link #computeEffectiveGranularitySpec}.
   */
  @JsonIgnore
  public GranularitySpec getGranularitySpec()
  {
    return effectiveGranularitySpec;
  }

  /**
   * The segment-partitioning granularity (segment granularity + intervals) when this schema is in baseTable mode and
   * declares one, else null. Only meaningful alongside {@link #getBaseTable()}.
   */
  @JsonProperty("segmentGranularitySpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public SegmentGranularitySpec getSegmentGranularitySpec()
  {
    return segmentGranularitySpec;
  }

  @JsonProperty
  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  @JsonProperty("baseTable")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public BaseTableProjectionSpec getBaseTable()
  {
    return baseTable;
  }

  /**
   * Canonical view of the base-table shape: the operator-declared {@link BaseTableProjectionSpec}, or a
   * synthesized {@link AdaptedBaseTableProjectionSpec} wrapping the legacy top-level fields. Always non-null.
   */
  @JsonIgnore
  public BaseTableProjectionSpec getEffectiveBaseTableSpec()
  {
    return effectiveBaseTableSpec;
  }

  /**
   * The clustered-value-groups base-table spec when this schema declares one, else null.
   */
  @JsonIgnore
  @Nullable
  public ClusteredValueGroupsBaseTableProjectionSpec getClusterSpec()
  {
    if (effectiveBaseTableSpec instanceof ClusteredValueGroupsBaseTableProjectionSpec) {
      return (ClusteredValueGroupsBaseTableProjectionSpec) effectiveBaseTableSpec;
    }
    return null;
  }

  @JsonProperty("dimensionsSpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @SuppressWarnings("unused") // serialization-only; legacy callers use getDimensionsSpec()
  private DimensionsSpec getDimensionsSpecForJson()
  {
    return dimensionsSpec;
  }

  @JsonProperty("metricsSpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @SuppressWarnings("unused") // serialization-only; legacy callers use getAggregators()
  private AggregatorFactory[] getAggregatorsForJson()
  {
    return baseTable != null ? null : aggregators;
  }

  @JsonProperty("granularitySpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @SuppressWarnings("unused") // serialization-only; legacy callers use getGranularitySpec()
  private GranularitySpec getGranularitySpecForJson()
  {
    return granularitySpec;
  }

  @Nullable
  public List<String> getProjectionNames()
  {
    if (projections == null) {
      return null;
    }
    return projections.stream().map(AggregateProjectionSpec::getName).collect(Collectors.toList());
  }

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    if (baseTable != null) {
      // In baseTable mode the query-granularity lives on the spec; only the partition-time segment granularity +
      // intervals are settable here, so translate into a SegmentGranularitySpec
      final Granularity segmentGranularity = granularitySpec instanceof UniformGranularitySpec
                                             ? granularitySpec.getSegmentGranularity()
                                             : BaseGranularitySpec.DEFAULT_SEGMENT_GRANULARITY;
      return builder(this)
          .withSegmentGranularity(new SegmentGranularitySpec(segmentGranularity, granularitySpec.inputIntervals()))
          .build();
    }
    return builder(this).withGranularity(granularitySpec).build();
  }

  public DataSchema withTransformSpec(TransformSpec transformSpec)
  {
    return builder(this).withTransform(transformSpec).build();
  }

  public DataSchema withDimensionsSpec(DimensionsSpec dimensionsSpec)
  {
    if (baseTable != null) {
      // Dimensions in baseTable mode are owned by the baseTable spec (clustering + non-clustering), which a flat
      // DimensionsSpec can't reconstruct.
      throw DruidException.defensive(
          "Cannot override dimensionsSpec on a baseTable-mode DataSchema; dimensions are defined by the baseTable spec"
      );
    }
    return builder(this).withDimensions(dimensionsSpec).build();
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", granularitySpec=" + granularitySpec +
           ", segmentGranularitySpec=" + segmentGranularitySpec +
           ", transformSpec=" + transformSpec +
           ", timestampSpec=" + timestampSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", projections=" + projections +
           ", baseTable=" + baseTable +
           '}';
  }



  private static void validateDatasourceName(String dataSource)
  {
    IdUtils.validateId("dataSource", dataSource);
  }

  /**
   * Computes the {@link DimensionsSpec} that we will actually use. It is derived from, but not necessarily identical
   * to, the one that we were given.
   */
  private static DimensionsSpec computeDimensionsSpec(
      final TimestampSpec timestampSpec,
      final DimensionsSpec dimensionsSpec,
      final AggregatorFactory[] aggregators
  )
  {
    final Set<String> inputFieldNames = computeInputFieldNames(timestampSpec, dimensionsSpec, aggregators);
    final Set<String> outputFieldNames = computeAndValidateOutputFieldNames(dimensionsSpec, aggregators);

    // Set up additional exclusions: all inputs and outputs, minus defined dimensions.
    final Set<String> additionalDimensionExclusions = new HashSet<>();
    additionalDimensionExclusions.addAll(inputFieldNames);
    additionalDimensionExclusions.addAll(outputFieldNames);
    additionalDimensionExclusions.removeAll(dimensionsSpec.getDimensionNames());

    return dimensionsSpec.withDimensionExclusions(additionalDimensionExclusions);
  }

  private static Set<String> computeInputFieldNames(
      final TimestampSpec timestampSpec,
      final DimensionsSpec dimensionsSpec,
      final AggregatorFactory[] aggregators
  )
  {
    final Set<String> fields = new HashSet<>();

    fields.add(timestampSpec.getTimestampColumn());
    fields.addAll(dimensionsSpec.getDimensionNames());
    Arrays.stream(aggregators)
          .flatMap(aggregator -> aggregator.requiredFields().stream())
          .forEach(fields::add);

    return fields;
  }

  /**
   * Computes the set of field names that are specified by the provided dimensions and aggregator lists.
   * <p>
   * If either list is null, it is ignored.
   *
   * @throws IllegalArgumentException if there are duplicate field names, or if any dimension or aggregator
   *                                  has a null name
   */
  private static Set<String> computeAndValidateOutputFieldNames(
      @Nullable final DimensionsSpec dimensionsSpec,
      @Nullable final AggregatorFactory[] aggregators
  )
  {
    // Field name -> where it was seen
    final Map<String, Multiset<String>> fields = new TreeMap<>();

    fields.computeIfAbsent(ColumnHolder.TIME_COLUMN_NAME, k -> TreeMultiset.create()).add(
        StringUtils.format(
            "primary timestamp (%s cannot appear elsewhere except as long-typed dimension)",
            ColumnHolder.TIME_COLUMN_NAME
        )
    );

    if (dimensionsSpec != null) {
      boolean sawTimeDimension = false;

      for (int i = 0; i < dimensionsSpec.getDimensions().size(); i++) {
        final DimensionSchema dimSchema = dimensionsSpec.getDimensions().get(i);
        final String field = dimSchema.getName();
        if (Strings.isNullOrEmpty(field)) {
          throw DruidException
              .forPersona(DruidException.Persona.USER)
              .ofCategory(DruidException.Category.INVALID_INPUT)
              .build("Encountered dimension with null or empty name at position[%d]", i);
        }

        if (ColumnHolder.TIME_COLUMN_NAME.equals(field)) {
          if (i > 0 && dimensionsSpec.isForceSegmentSortByTime()) {
            throw DruidException
                .forPersona(DruidException.Persona.USER)
                .ofCategory(DruidException.Category.INVALID_INPUT)
                .build(
                    "Encountered dimension[%s] at position[%d]. This is only supported when the dimensionsSpec "
                    + "parameter[%s] is set to[false]. %s",
                    field,
                    i,
                    DimensionsSpec.PARAMETER_FORCE_TIME_SORT,
                    DimensionsSpec.WARNING_NON_TIME_SORT_ORDER
                );
          } else if (!dimSchema.getColumnType().is(ValueType.LONG)) {
            throw DruidException
                .forPersona(DruidException.Persona.USER)
                .ofCategory(DruidException.Category.INVALID_INPUT)
                .build(
                    "Encountered dimension[%s] with incorrect type[%s]. Type must be 'long'.",
                    field,
                    dimSchema.getColumnType()
                );
          } else if (!sawTimeDimension) {
            // Skip adding __time to "fields" (once) if it's listed as a dimension, so it doesn't show up as an error.
            sawTimeDimension = true;
            continue;
          }
        }

        fields.computeIfAbsent(field, k -> TreeMultiset.create()).add("dimensions list");
      }
    }

    if (aggregators != null) {
      for (int i = 0; i < aggregators.length; i++) {
        final String field = aggregators[i].getName();
        if (Strings.isNullOrEmpty(field)) {
          throw new IAE("Encountered metric with null or empty name at position %d", i);
        }

        fields.computeIfAbsent(field, k -> TreeMultiset.create()).add("metricsSpec list");
      }
    }

    return getFieldsOrThrowIfErrors(fields);
  }

  /**
   * Validates that each {@link AggregateProjectionSpec} does not have duplicate column names in
   * {@link AggregateProjectionSpec#groupingColumns} and {@link AggregateProjectionSpec#aggregators} and that segment
   * {@link Granularity} is at least as coarse as {@link AggregateProjectionSchema#effectiveGranularity}
   */
  public static void validateProjections(
      @Nullable List<AggregateProjectionSpec> projections,
      @Nullable Granularity segmentGranularity
  )
  {
    if (projections != null) {
      final Set<String> names = Sets.newHashSetWithExpectedSize(projections.size());
      for (AggregateProjectionSpec projection : projections) {
        if (names.contains(projection.getName())) {
          throw InvalidInput.exception("projection[%s] is already defined, projection names must be unique", projection.getName());
        }
        names.add(projection.getName());
        final AggregateProjectionSchema schema = projection.toMetadataSchema();

        if (schema.getTimeColumnName() != null) {
          final Granularity projectionGranularity = schema.getEffectiveGranularity();
          if (segmentGranularity != null) {
            if (segmentGranularity.isFinerThan(projectionGranularity)) {
              throw InvalidInput.exception(
                  "projection[%s] has granularity[%s] which must be finer than or equal to segment granularity[%s]",
                  projection.getName(),
                  projectionGranularity,
                  segmentGranularity
              );
            }
          }
        }

        final Map<String, Multiset<String>> fields = new TreeMap<>();
        int position = 0;
        for (DimensionSchema grouping : projection.getGroupingColumns()) {
          final String field = grouping.getName();
          if (Strings.isNullOrEmpty(field)) {
            throw DruidException
                .forPersona(DruidException.Persona.USER)
                .ofCategory(DruidException.Category.INVALID_INPUT)
                .build("Encountered grouping column with null or empty name at position[%d]", position);
          }
          fields.computeIfAbsent(field, k -> TreeMultiset.create()).add("projection[" + projection.getName() + "] grouping column list");
          position++;
        }
        for (AggregatorFactory aggregator : projection.getAggregators()) {
          final String field = aggregator.getName();
          if (Strings.isNullOrEmpty(field)) {
            throw DruidException
                .forPersona(DruidException.Persona.USER)
                .ofCategory(DruidException.Category.INVALID_INPUT)
                .build("Encountered aggregator with null or empty name at position[%d]", position);
          }

          fields.computeIfAbsent(field, k -> TreeMultiset.create()).add("projection[" + projection.getName() + "] aggregators list");
          position++;
        }

        getFieldsOrThrowIfErrors(fields);
      }
    }
  }

  /**
   * Helper method that processes a validation result stored as a {@link Map} of field names to {@link Multiset} of
   * where they were defined. An error is indicated by the multi-set having more than a single entry
   * (such as if a field is defined as both a dimension and an aggregator). If all fields have only a single entry, this
   * method returns the list of output field names. If there are duplicates, this method throws a {@link DruidException}
   * collecting all validation errors to help indicate where a field is defined
   *
   * @see #computeAndValidateOutputFieldNames
   * @see #validateProjections(List, Granularity)
   */
  private static Set<String> getFieldsOrThrowIfErrors(Map<String, Multiset<String>> validatedFields)
  {
    final List<String> errors = new ArrayList<>();

    for (Map.Entry<String, Multiset<String>> fieldEntry : validatedFields.entrySet()) {
      if (fieldEntry.getValue().entrySet().stream().mapToInt(Multiset.Entry::getCount).sum() > 1) {
        errors.add(
            StringUtils.format(
                "[%s] seen in %s",
                fieldEntry.getKey(),
                fieldEntry.getValue().entrySet().stream().map(
                    entry ->
                        StringUtils.format(
                            "%s%s",
                            entry.getElement(),
                            entry.getCount() == 1 ? "" : StringUtils.format(
                                " (%d occurrences)",
                                entry.getCount()
                            )
                        )
                ).collect(Collectors.joining(", "))
            )
        );
      }
    }

    if (errors.isEmpty()) {
      return validatedFields.keySet();
    } else {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("Cannot specify a column more than once: %s", String.join("; ", errors));
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSchema that = (DataSchema) o;
    return Objects.equals(dataSource, that.dataSource) &&
           Objects.deepEquals(aggregators, that.aggregators) &&
           Objects.equals(granularitySpec, that.granularitySpec) &&
           Objects.equals(segmentGranularitySpec, that.segmentGranularitySpec) &&
           Objects.equals(transformSpec, that.transformSpec) &&
           Objects.equals(timestampSpec, that.timestampSpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Objects.equals(projections, that.projections) &&
           Objects.equals(baseTable, that.baseTable);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        Arrays.hashCode(aggregators),
        granularitySpec,
        segmentGranularitySpec,
        transformSpec,
        timestampSpec,
        dimensionsSpec,
        projections,
        baseTable
    );
  }

  public static class Builder
  {
    private String dataSource;
    private AggregatorFactory[] aggregators;
    private GranularitySpec granularitySpec;
    private SegmentGranularitySpec segmentGranularitySpec;
    private TransformSpec transformSpec;
    private TimestampSpec timestampSpec;
    private DimensionsSpec dimensionsSpec;
    private List<AggregateProjectionSpec> projections;
    private BaseTableProjectionSpec baseTable;

    public Builder()
    {

    }

    public Builder(DataSchema schema)
    {
      this.dataSource = schema.dataSource;
      this.timestampSpec = schema.timestampSpec;
      this.dimensionsSpec = schema.dimensionsSpec;
      this.transformSpec = schema.transformSpec;
      this.aggregators = schema.aggregators;
      this.projections = schema.projections;
      this.granularitySpec = schema.granularitySpec;
      this.segmentGranularitySpec = schema.segmentGranularitySpec;
      this.baseTable = schema.baseTable;
    }

    public Builder withDataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder withTimestamp(TimestampSpec timestampSpec)
    {
      this.timestampSpec = timestampSpec;
      return this;
    }

    public Builder withDimensions(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder withDimensions(List<DimensionSchema> dimensions)
    {
      this.dimensionsSpec = DimensionsSpec.builder().setDimensions(dimensions).build();
      return this;
    }

    public Builder withDimensions(DimensionSchema... dimensions)
    {
      return withDimensions(Arrays.asList(dimensions));
    }

    public Builder withAggregators(AggregatorFactory... aggregators)
    {
      this.aggregators = aggregators;
      return this;
    }

    public Builder withGranularity(GranularitySpec granularitySpec)
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Builder withSegmentGranularity(SegmentGranularitySpec segmentGranularitySpec)
    {
      this.segmentGranularitySpec = segmentGranularitySpec;
      return this;
    }

    public Builder withTransform(TransformSpec transformSpec)
    {
      this.transformSpec = transformSpec;
      return this;
    }

    public Builder withBaseTable(BaseTableProjectionSpec baseTable)
    {
      this.baseTable = baseTable;
      return this;
    }

    public Builder withProjections(List<AggregateProjectionSpec> projections)
    {
      this.projections = projections;
      return this;
    }

    public DataSchema build()
    {
      return new DataSchema(
          dataSource,
          timestampSpec,
          dimensionsSpec,
          aggregators,
          granularitySpec,
          segmentGranularitySpec,
          transformSpec,
          projections,
          baseTable,
          null
      );
    }
  }
}
