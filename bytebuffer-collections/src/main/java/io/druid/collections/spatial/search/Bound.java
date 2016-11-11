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

package io.druid.collections.spatial.search;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.collections.spatial.ImmutableNode;
import io.druid.collections.spatial.ImmutablePoint;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "rectangular", value = RectangularBound.class),
    @JsonSubTypes.Type(name = "radius", value = RadiusBound.class),
    @JsonSubTypes.Type(name = "polygon", value = PolygonBound.class)
})
public interface Bound
{
  public int getLimit();

  public int getNumDims();

  public boolean overlaps(ImmutableNode node);

  public boolean contains(float[] coords);

  public Iterable<ImmutablePoint> filter(Iterable<ImmutablePoint> points);

  public byte[] getCacheKey();
}
