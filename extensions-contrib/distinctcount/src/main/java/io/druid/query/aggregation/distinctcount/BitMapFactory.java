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

package io.druid.query.aggregation.distinctcount;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.collections.bitmap.MutableBitmap;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RoaringBitMapFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "java", value = JavaBitMapFactory.class),
    @JsonSubTypes.Type(name = "concise", value = ConciseBitMapFactory.class),
    @JsonSubTypes.Type(name = "roaring", value = RoaringBitMapFactory.class)
})
public interface BitMapFactory
{
  /**
   * Create a new empty bitmap
   *
   * @return the new bitmap
   */
  public MutableBitmap makeEmptyMutableBitmap();
}
