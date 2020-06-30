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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

/**
 * Provides information about a dimension for a grouping query, like topN or groupBy. Note that this is not annotated
 * with {@code PublicApi}, since it is not meant to be stable for usage by non-built-in queries.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDimensionSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultDimensionSpec.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimensionSpec.class),
    @JsonSubTypes.Type(name = "regexFiltered", value = RegexFilteredDimensionSpec.class),
    @JsonSubTypes.Type(name = "listFiltered", value = ListFilteredDimensionSpec.class),
    @JsonSubTypes.Type(name = "prefixFiltered", value = PrefixFilteredDimensionSpec.class)
})
@SubclassesMustOverrideEqualsAndHashCode
public interface DimensionSpec extends Cacheable
{
  String getDimension();

  String getOutputName();

  ValueType getOutputType();

  //ExtractionFn can be implemented with decorate(..) fn
  @Deprecated
  @Nullable
  ExtractionFn getExtractionFn();

  DimensionSelector decorate(DimensionSelector selector);

  default SingleValueDimensionVectorSelector decorate(SingleValueDimensionVectorSelector selector)
  {
    throw new UOE("DimensionSpec[%s] cannot vectorize", getClass().getName());
  }

  default MultiValueDimensionVectorSelector decorate(MultiValueDimensionVectorSelector selector)
  {
    throw new UOE("DimensionSpec[%s] cannot vectorize", getClass().getName());
  }

  /**
   * Does this DimensionSpec require that decorate() be called to produce correct results?
   */
  boolean mustDecorate();

  /**
   * Does this DimensionSpec have working {@link #decorate(SingleValueDimensionVectorSelector)} and
   * {@link #decorate(MultiValueDimensionVectorSelector)} methods?
   */
  default boolean canVectorize()
  {
    return false;
  }

  boolean preservesOrdering();

  /**
   * Returns a copy of this DimensionSpec with the underlying dimension (the value of {@link #getDimension()})
   * replaced by "newDimension".
   */
  DimensionSpec withDimension(String newDimension);
}
