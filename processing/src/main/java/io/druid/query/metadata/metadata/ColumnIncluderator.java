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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "none", value= NoneColumnIncluderator.class),
    @JsonSubTypes.Type(name = "all", value= AllColumnIncluderator.class),
    @JsonSubTypes.Type(name = "list", value= ListColumnIncluderator.class)
})
public interface ColumnIncluderator
{
  public static final byte[] NONE_CACHE_PREFIX = new byte[]{0x0};
  public static final byte[] ALL_CACHE_PREFIX = new byte[]{0x1};
  public static final byte[] LIST_CACHE_PREFIX = new byte[]{0x2};

  public boolean include(String columnName);
  public byte[] getCacheKey();
}
