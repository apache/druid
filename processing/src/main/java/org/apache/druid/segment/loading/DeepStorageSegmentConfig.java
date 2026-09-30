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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * The {@code druid.storage} properties that describe how segments are laid out in deep storage rather than how to
 * reach it, and so are shared by every {@link DataSegmentPusher} instead of belonging to any one of them. Currently
 * that is only {@code druid.storage.zip}, but anything else that is a property of the layout rather than of a
 * particular deep storage belongs here too.
 * <p>
 * This is bound once in core, by {@code LocalDataStorageDruidModule}, instead of once per deep storage
 * implementation, so that these properties mean the same thing whatever {@code druid.storage.type} is set to, and so
 * that each implementation does not have to redeclare them in its own config class. Any single-implementation
 * property still belongs in that implementation's own config, next to its bucket and credentials.
 * <p>
 * Values here are deliberately tri-state: when a property is unset, the getter falls back to the default the calling
 * implementation passes in, since those defaults differ per implementation.
 */
public class DeepStorageSegmentConfig
{
  @JsonProperty
  @Nullable
  private final Boolean zip;

  @JsonCreator
  public DeepStorageSegmentConfig(@JsonProperty("zip") @Nullable Boolean zip)
  {
    this.zip = zip;
  }

  public DeepStorageSegmentConfig()
  {
    this(null);
  }

  /**
   * Whether segments should be written as a zip file, falling back to {@code defaultZip} when
   * {@code druid.storage.zip} has not been set.
   */
  public boolean isZip(boolean defaultZip)
  {
    return zip == null ? defaultZip : zip;
  }

  @JsonProperty("zip")
  @Nullable
  public Boolean getZip()
  {
    return zip;
  }
}
