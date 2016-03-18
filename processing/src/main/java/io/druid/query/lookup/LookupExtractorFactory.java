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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Supplier;
import io.druid.query.extraction.MapLookupExtractorFactory;

import javax.annotation.Nullable;

/**
 * Users of Lookup Extraction need to implement a {@link LookupExtractorFactory} supplier of type {@link LookupExtractor}.
 * Such factory will manage the state and life cycle of an given lookup.
 * If a LookupExtractorFactory wishes to support idempotent updates, it needs to implement the  `replaces` method
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapLookupExtractorFactory.class)
})
public interface LookupExtractorFactory extends Supplier<LookupExtractor>
{
  /**
   * <p>
   *   This method will be called to start the LookupExtractor upon registered
   *   Calling start multiple times should not lead to any failure and suppose to return true in both cases.
   * </p>
   *
   * @return true if start successfully started the {@link LookupExtractor}
   */
  public boolean start();

  /**
   * <p>
   *   This method will be called to stop the LookupExtractor upon deletion.
   *   Calling this method multiple times should not lead to any failure.
   * </p>
   * @return true if successfully closed the {@link LookupExtractor}
   */
  public boolean close();

  /**
   * Determine if this LookupExtractorFactory should replace some other LookupExtractorFactory.
   * This is used to implement no-down-time
   * @param other Some other LookupExtractorFactory which might need replaced
   * @return `true` if the other should be replaced by this one. `false` if this one should not replace the other factory
   */
  boolean replaces(@Nullable LookupExtractorFactory other);
}
