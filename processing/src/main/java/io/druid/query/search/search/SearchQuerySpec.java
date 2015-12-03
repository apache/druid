/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "contains", value = ContainsSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "insensitive_contains", value = InsensitiveContainsSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "fragment", value = FragmentSearchQuerySpec.class),
    @JsonSubTypes.Type(name = "regex", value = RegexSearchQuerySpec.class)
})
public interface SearchQuerySpec
{
  public boolean accept(String dimVal);

  public byte[] getCacheKey();
}
