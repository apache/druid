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

/**
 * A row transform that is part of a {@link TransformSpec}. Transforms allow adding new fields to input rows. Each
 * one has a "name" (the name of the new field) which can be referred to by DimensionSpecs, AggregatorFactories, etc.
 * Each also has a "row function", which produces values for this new field based on looking at the entire input row.
 *
 * If a transform has the same name as a field in an input row, then it will shadow the original field. Transforms
 * that shadow fields may still refer to the fields they shadow. This can be used to transform a field "in-place".
 *
 * Transforms do have some limitations. They can only refer to fields present in the actual input rows; in particular,
 * they cannot refer to other transforms. And they cannot remove fields, only add them. However, they can shadow a
 * field with another field containing all nulls, which will act similarly to removing the field.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "expression", value = ExpressionTransform.class)
})
public interface Transform
{
  /**
   * Returns the field name for this transform.
   */
  String getName();

  /**
   * Returns the function for this transform. The RowFunction takes an entire row as input and returns a column value
   * as output.
   */
  RowFunction getRowFunction();
}
