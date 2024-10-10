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

package org.apache.druid.collections.spatial.search;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.collections.spatial.ImmutableNode;

import javax.annotation.Nullable;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "rectangular", value = RectangularBound.class),
    @JsonSubTypes.Type(name = "radius", value = RadiusBound.class),
    @JsonSubTypes.Type(name = "polygon", value = PolygonBound.class)
})
@SubclassesMustOverrideEqualsAndHashCode
public interface Bound<TCoordinateArray, TPoint extends ImmutableNode<TCoordinateArray>>
{
  int getLimit();

  int getNumDims();

  boolean overlaps(ImmutableNode<TCoordinateArray> node);

  boolean contains(TCoordinateArray coords);

  /***
   * containsObj is mainly used to create object matechers on top custom/extensible spatial column,
   * it receives it as object and corresponding implementations need to logic to unpack the objects and invoke contains
   * @param input Takes an object spatial column as input
   * @return boolean value if it falls within given bound
   */
  boolean containsObj(@Nullable Object input);

  Iterable<TPoint> filter(Iterable<TPoint> points);

  byte[] getCacheKey();
}
