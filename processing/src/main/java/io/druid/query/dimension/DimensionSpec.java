/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.Cacheable;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ValueType;

/**
 * Provides information about a dimension for a grouping query, like topN or groupBy. Note that this is not annotated
 * with {@code PublicApi}, since it is not meant to be stable for usage by non-built-in queries.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDimensionSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultDimensionSpec.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimensionSpec.class),
    @JsonSubTypes.Type(name = "regexFiltered", value = RegexFilteredDimensionSpec.class),
    @JsonSubTypes.Type(name = "listFiltered", value = ListFilteredDimensionSpec.class)
})
public interface DimensionSpec extends Cacheable
{
  String getDimension();

  String getOutputName();

  ValueType getOutputType();

  //ExtractionFn can be implemented with decorate(..) fn
  @Deprecated
  ExtractionFn getExtractionFn();

  DimensionSelector decorate(DimensionSelector selector);

  /**
   * Does this DimensionSpec require that decorate() be called to produce correct results?
   */
  boolean mustDecorate();

  boolean preservesOrdering();
}
