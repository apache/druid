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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "contains", value = ContainsSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "insensitive_contains", value = InsensitiveContainsSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "fragment", value = FragmentSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "regex", value = RegexSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "all", value = AllSearchQuerySpec.class)
})
public interface SearchQuerySpec
{
  public boolean accept(@Nullable String dimVal);

  public byte[] getCacheKey();
}
