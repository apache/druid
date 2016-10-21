/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.druid.java.util.common.parsers.ParserUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DimensionsSpec
{
  private final List<DimensionSchema> dimensions;
  private final Set<String> dimensionExclusions;
  private final Map<String, DimensionSchema> dimensionSchemaMap;

  public static List<DimensionSchema> getDefaultSchemas(List<String> dimNames)
  {
    return Lists.transform(
        dimNames,
        new Function<String, DimensionSchema>()
        {
          @Override
          public DimensionSchema apply(String input)
          {
            return new StringDimensionSchema(input);
          }
        }
    );
  }

  public static DimensionSchema convertSpatialSchema(SpatialDimensionSchema spatialSchema)
  {
    return new NewSpatialDimensionSchema(spatialSchema.getDimName(), spatialSchema.getDims());
  }

  @JsonCreator
  public DimensionsSpec(
      @JsonProperty("dimensions") List<DimensionSchema> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
      @Deprecated @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
  )
  {
    this.dimensions = dimensions == null
                      ? Lists.<DimensionSchema>newArrayList()
                      : Lists.newArrayList(dimensions);

    this.dimensionExclusions = (dimensionExclusions == null)
                               ? Sets.<String>newHashSet()
                               : Sets.newHashSet(dimensionExclusions);

    List<SpatialDimensionSchema> spatialDims = (spatialDimensions == null)
                                               ? Lists.<SpatialDimensionSchema>newArrayList()
                                               : spatialDimensions;

    verify(spatialDims);

    // Map for easy dimension name-based schema lookup
    this.dimensionSchemaMap = new HashMap<>();
    for (DimensionSchema schema : this.dimensions) {
      dimensionSchemaMap.put(schema.getName(), schema);
    }

    for(SpatialDimensionSchema spatialSchema : spatialDims) {
      DimensionSchema newSchema = DimensionsSpec.convertSpatialSchema(spatialSchema);
      this.dimensions.add(newSchema);
      dimensionSchemaMap.put(newSchema.getName(), newSchema);
    }
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

  @Deprecated @JsonIgnore
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    Iterable<NewSpatialDimensionSchema> filteredList = Iterables.filter(
        dimensions, NewSpatialDimensionSchema.class
    );

    Iterable<SpatialDimensionSchema> transformedList = Iterables.transform(
        filteredList,
        new Function<NewSpatialDimensionSchema, SpatialDimensionSchema>()
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
        new Function<DimensionSchema, String>()
        {
          @Override
          public String apply(DimensionSchema input)
          {
            return input.getName();
          }
        }
    );
  }

  public DimensionSchema getSchema(String dimension)
  {
    return dimensionSchemaMap.get(dimension);
  }

  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  public DimensionsSpec withDimensions(List<DimensionSchema> dims)
  {
    return new DimensionsSpec(dims, ImmutableList.copyOf(dimensionExclusions), null);
  }

  public DimensionsSpec withDimensionExclusions(Set<String> dimExs)
  {
    return new DimensionsSpec(
        dimensions,
        ImmutableList.copyOf(Sets.union(dimensionExclusions, dimExs)),
        null
    );
  }

  @Deprecated
  public DimensionsSpec withSpatialDimensions(List<SpatialDimensionSchema> spatials)
  {
    return new DimensionsSpec(dimensions, ImmutableList.copyOf(dimensionExclusions), spatials);
  }

  private void verify(List<SpatialDimensionSchema> spatialDimensions)
  {
    List<String> dimNames = getDimensionNames();
    Preconditions.checkArgument(
        Sets.intersection(this.dimensionExclusions, Sets.newHashSet(dimNames)).isEmpty(),
        "dimensions and dimensions exclusions cannot overlap"
    );

    ParserUtils.validateFields(dimNames);
    ParserUtils.validateFields(dimensionExclusions);

    List<String> spatialDimNames = Lists.transform(
        spatialDimensions,
        new Function<SpatialDimensionSchema, String>()
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

    if (!dimensions.equals(that.dimensions)) {
      return false;
    }

    return dimensionExclusions.equals(that.dimensionExclusions);
  }

  @Override
  public int hashCode()
  {
    int result = dimensions.hashCode();
    result = 31 * result + dimensionExclusions.hashCode();
    return result;
  }
}
