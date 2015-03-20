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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "time", value = TimeDimExtractionFn.class),
    @JsonSubTypes.Type(name = "regex", value = RegexDimExtractionFn.class),
    @JsonSubTypes.Type(name = "partial", value = MatchingDimExtractionFn.class),
    @JsonSubTypes.Type(name = "searchQuery", value = SearchQuerySpecDimExtractionFn.class),
    @JsonSubTypes.Type(name = "javascript", value = JavascriptExtractionFn.class),
    @JsonSubTypes.Type(name = "timeFormat", value = TimeFormatExtractionFn.class)
})
/**
 * An ExtractionFn is a function that can be used to transform the values of a column (typically a dimension)
 *
 * A simple example of the type of operation this enables is the RegexDimExtractionFn which applies a
 * regular expression with a capture group.  When the regular expression matches the value of a dimension,
 * the value captured by the group is used for grouping operations instead of the dimension value.
 */
public interface ExtractionFn
{
  /**
   * Returns a byte[] unique to all concrete implementations of DimExtractionFn.  This byte[] is used to
   * generate a cache key for the specific query.
   *
   * @return a byte[] unit to all concrete implements of DimExtractionFn
   */
  public byte[] getCacheKey();

  /**
   * The "extraction" function.  This should map a value into some other String value.
   *
   * In order to maintain the "null and empty string are equivalent" semantics that Druid provides, the
   * empty string is considered invalid output for this method and should instead return null.  This is
   * a contract on the method rather than enforced at a lower level in order to eliminate a global check
   * for extraction functions that do not already need one.
   *
   *
   * @param value the original value of the dimension
   * @return a value that should be used instead of the original
   */
  public String apply(Object value);

  public String apply(String value);

  public String apply(long value);

  /**
   * Offers information on whether the extraction will preserve the original ordering of the values.
   *
   * Some optimizations of queries is possible if ordering is preserved.  Null values *do* count towards
   * ordering.
   *
   * @return true if ordering is preserved, false otherwise
   */
  public boolean preservesOrdering();
}
