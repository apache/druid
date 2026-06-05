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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Specification for how input rows should be transformed during ingestion. This is the base interface
 * for the {@code transformSpec} field in {@link org.apache.druid.segment.indexing.DataSchema}.
 *
 * <p>Two implementations are provided:
 * <ul>
 *   <li>{@link TransformSpec} — the default, for expression-based transforms and filters</li>
 *   <li>{@link ScanTransformSpec} — for scan-query-based transforms (unnest, virtual columns, filters)</li>
 * </ul>
 *
 * <p>When no {@code "type"} is specified in JSON, the default {@link TransformSpec} is used for backward
 * compatibility.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TransformSpec.class)
@JsonSubTypes({
    @JsonSubTypes.Type(name = "scan", value = ScanTransformSpec.class)
})
public interface BaseTransformSpec
{
  /**
   * Creates a {@link BaseTransformer} that applies this spec's transforms to input rows.
   */
  BaseTransformer toTransformer();

  /**
   * Wraps an {@link InputSourceReader} with this spec's transforms applied to each row.
   */
  default InputSourceReader decorate(InputSourceReader reader)
  {
    return new TransformingInputSourceReader(reader, toTransformer());
  }

  /**
   * Returns the names of all input columns required by this spec's transforms and filters.
   */
  Set<String> getRequiredColumns();

  /**
   * Returns the list of individual {@link Transform} objects, if applicable.
   * Defaults to an empty list for specs that don't use the transforms list (e.g., {@link ScanTransformSpec}).
   */
  default List<Transform> getTransforms()
  {
    return List.of();
  }

  /**
   * Returns the filter applied to input rows, if applicable.
   * Defaults to null for specs that handle filtering internally (e.g., {@link ScanTransformSpec}).
   */
  @Nullable
  default DimFilter getFilter()
  {
    return null;
  }
}
