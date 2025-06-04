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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@PublicApi
public class DimensionsSpec
{
  /**
   * Parameter name for allowing any sort order. Also used as an MSQ context parameter, for some consistency between
   * MSQ and native ingest configuration.
   */
  public static final String PARAMETER_FORCE_TIME_SORT = "forceSegmentSortByTime";

  /**
   * Warning about non-time ordering to include in error messages when {@link #PARAMETER_FORCE_TIME_SORT} is
   * not set.
   */
  public static final String WARNING_NON_TIME_SORT_ORDER = StringUtils.format(
      "Warning: support for segments not sorted by[%s] is experimental. Such segments are not readable by older "
      + "version of Druid, and certain queries cannot run on them. See "
      + "https://druid.apache.org/docs/latest/ingestion/partitioning#sorting for details before setting "
      + "%s to[false].",
      ColumnHolder.TIME_COLUMN_NAME,
      PARAMETER_FORCE_TIME_SORT
  );

  public static final boolean DEFAULT_FORCE_TIME_SORT = true;

  private final List<DimensionSchema> dimensions;
  private final Set<String> dimensionExclusions;
  private final Map<String, DimensionSchema> dimensionSchemaMap;
  private final boolean includeAllDimensions;
  private final Boolean forceSegmentSortByTime;

  private final boolean useSchemaDiscovery;

  public static final DimensionsSpec EMPTY = new DimensionsSpec(null, null, null, false, null, false);

  public static List<DimensionSchema> getDefaultSchemas(List<String> dimNames)
  {
    return getDefaultSchemas(dimNames, DimensionSchema.MultiValueHandling.ofDefault());
  }

  public static List<DimensionSchema> getDefaultSchemas(
      final List<String> dimNames,
      final DimensionSchema.MultiValueHandling multiValueHandling
  )
  {
    return dimNames.stream()
                   .map(input -> new StringDimensionSchema(input, multiValueHandling, true))
                   .collect(Collectors.toList());
  }

  public static DimensionSchema convertSpatialSchema(SpatialDimensionSchema spatialSchema)
  {
    return new NewSpatialDimensionSchema(spatialSchema.getDimName(), spatialSchema.getDims());
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public DimensionsSpec(List<DimensionSchema> dimensions)
  {
    this(dimensions, null, null, false, null, null);
  }

  @JsonCreator
  private DimensionsSpec(
      @JsonProperty("dimensions") List<DimensionSchema> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
      @Deprecated @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions,
      @JsonProperty("includeAllDimensions") boolean includeAllDimensions,
      @JsonProperty("useSchemaDiscovery") Boolean useSchemaDiscovery,
      @JsonProperty(PARAMETER_FORCE_TIME_SORT) Boolean forceSegmentSortByTime
  )
  {
    this.dimensions = dimensions == null
                      ? new ArrayList<>()
                      : Lists.newArrayList(dimensions);

    this.dimensionExclusions = (dimensionExclusions == null)
                               ? new HashSet<>()
                               : Sets.newHashSet(dimensionExclusions);

    List<SpatialDimensionSchema> spatialDims = (spatialDimensions == null)
                                               ? new ArrayList<>()
                                               : spatialDimensions;

    verify(spatialDims);

    // Map for easy dimension name-based schema lookup
    this.dimensionSchemaMap = new HashMap<>();
    for (DimensionSchema schema : this.dimensions) {
      dimensionSchemaMap.put(schema.getName(), schema);
    }

    for (SpatialDimensionSchema spatialSchema : spatialDims) {
      DimensionSchema newSchema = DimensionsSpec.convertSpatialSchema(spatialSchema);
      this.dimensions.add(newSchema);
      dimensionSchemaMap.put(newSchema.getName(), newSchema);
    }
    this.includeAllDimensions = includeAllDimensions;
    this.useSchemaDiscovery =
        useSchemaDiscovery != null && useSchemaDiscovery;
    this.forceSegmentSortByTime = forceSegmentSortByTime;
  }

  @JsonProperty
  public List<DimensionSchema> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Set<String> getDimensionExclusions()
  {
    return dimensionExclusions;
  }

  @JsonProperty
  public boolean isIncludeAllDimensions()
  {
    return includeAllDimensions;
  }

  @JsonProperty
  public boolean useSchemaDiscovery()
  {
    return useSchemaDiscovery;
  }

  @JsonProperty(PARAMETER_FORCE_TIME_SORT)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean isForceSegmentSortByTimeConfigured()
  {
    return forceSegmentSortByTime;
  }

  /**
   * Returns {@link #isForceSegmentSortByTimeConfigured()} if nonnull, otherwise
   * {@link #DEFAULT_FORCE_TIME_SORT}.
   */
  public boolean isForceSegmentSortByTime()
  {
    if (forceSegmentSortByTime != null) {
      return forceSegmentSortByTime;
    } else {
      return DEFAULT_FORCE_TIME_SORT;
    }
  }

  @Deprecated
  @JsonIgnore
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    Iterable<NewSpatialDimensionSchema> filteredList = Iterables.filter(dimensions, NewSpatialDimensionSchema.class);

    Iterable<SpatialDimensionSchema> transformedList = Iterables.transform(
        filteredList,
        new Function<>()
        {
          @Nullable
          @Override
          public SpatialDimensionSchema apply(NewSpatialDimensionSchema input)
          {
            return new SpatialDimensionSchema(input.getName(), input.getDims());
          }
        }
    );

    return Lists.newArrayList(transformedList);
  }


  @JsonIgnore
  public List<String> getDimensionNames()
  {
    return Lists.transform(
        dimensions,
        new Function<>()
        {
          @Override
          public String apply(DimensionSchema input)
          {
            return input.getName();
          }
        }
    );
  }

  @PublicApi
  public DimensionSchema getSchema(String dimension)
  {
    return dimensionSchemaMap.get(dimension);
  }

  /**
   * Whether this spec represents a set of fixed dimensions. Will be false if schema discovery is enabled, even if
   * some dimensions are explicitly defined.
   */
  public boolean hasFixedDimensions()
  {
    return dimensions != null && !dimensions.isEmpty() && !useSchemaDiscovery && !includeAllDimensions;
  }

  @PublicApi
  public DimensionsSpec withDimensions(List<DimensionSchema> dims)
  {
    return new DimensionsSpec(
        dims,
        ImmutableList.copyOf(dimensionExclusions),
        null,
        includeAllDimensions,
        useSchemaDiscovery,
        forceSegmentSortByTime
    );
  }

  public DimensionsSpec withDimensionExclusions(Set<String> dimExs)
  {
    return new DimensionsSpec(
        dimensions,
        ImmutableList.copyOf(Sets.union(dimensionExclusions, dimExs)),
        null,
        includeAllDimensions,
        useSchemaDiscovery,
        forceSegmentSortByTime
    );
  }

  @Deprecated
  public DimensionsSpec withSpatialDimensions(List<SpatialDimensionSchema> spatials)
  {
    return new DimensionsSpec(
        dimensions,
        ImmutableList.copyOf(dimensionExclusions),
        spatials,
        includeAllDimensions,
        useSchemaDiscovery,
        forceSegmentSortByTime
    );
  }

  private void verify(List<SpatialDimensionSchema> spatialDimensions)
  {
    List<String> dimNames = getDimensionNames();
    Preconditions.checkArgument(
        Sets.intersection(this.dimensionExclusions, Sets.newHashSet(dimNames)).isEmpty(),
        "dimensions and dimensions exclusions cannot overlap"
    );

    List<String> spatialDimNames = Lists.transform(
        spatialDimensions,
        new Function<>()
        {
          @Override
          public String apply(SpatialDimensionSchema input)
          {
            return input.getDimName();
          }
        }
    );

    // Don't allow duplicates between main list and deprecated spatial list
    ParserUtils.validateFields(Iterables.concat(dimNames, spatialDimNames));
    ParserUtils.validateFields(dimensionExclusions);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DimensionsSpec that = (DimensionsSpec) o;
    return includeAllDimensions == that.includeAllDimensions
           && useSchemaDiscovery == that.useSchemaDiscovery
           && Objects.equals(dimensions, that.dimensions)
           && Objects.equals(dimensionExclusions, that.dimensionExclusions)
           && Objects.equals(dimensionSchemaMap, that.dimensionSchemaMap)
           && Objects.equals(forceSegmentSortByTime, that.forceSegmentSortByTime);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dimensions,
        dimensionExclusions,
        dimensionSchemaMap,
        includeAllDimensions,
        forceSegmentSortByTime,
        useSchemaDiscovery
    );
  }

  @Override
  public String toString()
  {
    return "DimensionsSpec{" +
           "dimensions=" + dimensions +
           ", dimensionExclusions=" + dimensionExclusions +
           ", includeAllDimensions=" + includeAllDimensions +
           ", useSchemaDiscovery=" + useSchemaDiscovery +
           (forceSegmentSortByTime != null
            ? ", forceSegmentSortByTime=" + forceSegmentSortByTime
            : "") +
           '}';
  }

  public static final class Builder
  {
    private List<DimensionSchema> dimensions;
    private List<String> dimensionExclusions;
    private List<SpatialDimensionSchema> spatialDimensions;
    private boolean includeAllDimensions;
    private boolean useSchemaDiscovery;
    private Boolean forceSegmentSortByTime;

    public Builder setDimensions(List<DimensionSchema> dimensions)
    {
      this.dimensions = dimensions;
      return this;
    }

    public Builder setDefaultSchemaDimensions(List<String> dimensions)
    {
      this.dimensions = getDefaultSchemas(dimensions);
      return this;
    }

    public Builder setDimensionExclusions(List<String> dimensionExclusions)
    {
      this.dimensionExclusions = dimensionExclusions;
      return this;
    }

    @Deprecated
    public Builder setSpatialDimensions(List<SpatialDimensionSchema> spatialDimensions)
    {
      this.spatialDimensions = spatialDimensions;
      return this;
    }

    public Builder setIncludeAllDimensions(boolean includeAllDimensions)
    {
      this.includeAllDimensions = includeAllDimensions;
      return this;
    }

    public Builder useSchemaDiscovery(boolean useSchemaDiscovery)
    {
      this.useSchemaDiscovery = useSchemaDiscovery;
      return this;
    }

    public Builder setForceSegmentSortByTime(Boolean forceSegmentSortByTime)
    {
      this.forceSegmentSortByTime = forceSegmentSortByTime;
      return this;
    }

    public DimensionsSpec build()
    {
      return new DimensionsSpec(
          dimensions,
          dimensionExclusions,
          spatialDimensions,
          includeAllDimensions,
          useSchemaDiscovery,
          forceSegmentSortByTime
      );
    }
  }
}
