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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.guice.annotations.ExtensionPoint;
import io.druid.java.util.common.Cacheable;
import io.druid.query.lookup.LookupExtractionFn;

import javax.annotation.Nullable;

/**
 * An ExtractionFn is a function that can be used to transform the values of a column (typically a dimension).
 * Note that ExtractionFn implementations are expected to be Threadsafe.
 *
 * A simple example of the type of operation this enables is the RegexDimExtractionFn which applies a
 * regular expression with a capture group.  When the regular expression matches the value of a dimension,
 * the value captured by the group is used for grouping operations instead of the dimension value.
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "time", value = TimeDimExtractionFn.class),
    @JsonSubTypes.Type(name = "regex", value = RegexDimExtractionFn.class),
    @JsonSubTypes.Type(name = "partial", value = MatchingDimExtractionFn.class),
    @JsonSubTypes.Type(name = "searchQuery", value = SearchQuerySpecDimExtractionFn.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptExtractionFn.class),
    @JsonSubTypes.Type(name = "timeFormat", value = TimeFormatExtractionFn.class),
    @JsonSubTypes.Type(name = "identity", value = IdentityExtractionFn.class),
    @JsonSubTypes.Type(name = "lookup", value = LookupExtractionFn.class),
    @JsonSubTypes.Type(name = "substring", value = SubstringDimExtractionFn.class),
    @JsonSubTypes.Type(name = "cascade", value = CascadeExtractionFn.class),
    @JsonSubTypes.Type(name = "stringFormat", value = StringFormatExtractionFn.class),
    @JsonSubTypes.Type(name = "upper", value = UpperExtractionFn.class),
    @JsonSubTypes.Type(name = "lower", value = LowerExtractionFn.class),
    @JsonSubTypes.Type(name = "bucket", value = BucketExtractionFn.class),
    @JsonSubTypes.Type(name = "strlen", value = StrlenExtractionFn.class)
})
public interface ExtractionFn extends Cacheable
{
  /**
   * The "extraction" function.  This should map an Object into some String value.
   * <p>
   * In order to maintain the "null and empty string are equivalent" semantics that Druid provides, the
   * empty string is considered invalid output for this method and should instead return null.  This is
   * a contract on the method rather than enforced at a lower level in order to eliminate a global check
   * for extraction functions that do not already need one.
   *
   * @param value the original value of the dimension
   *
   * @return a value that should be used instead of the original
   */
  @Nullable
  String apply(@Nullable Object value);

  /**
   * The "extraction" function.  This should map a String value into some other String value.
   * <p>
   * Like {@link #apply(Object)}, the empty string is considered invalid output for this method and it should
   * instead return null.
   *
   * @param value the original value of the dimension
   *
   * @return a value that should be used instead of the original
   */
  @Nullable
  String apply(@Nullable String value);

  /**
   * The "extraction" function.  This should map a long value into some String value.
   * <p>
   * Like {@link #apply(Object)}, the empty string is considered invalid output for this method and it should
   * instead return null.
   *
   * @param value the original value of the dimension
   *
   * @return a value that should be used instead of the original
   */
  String apply(long value);

  /**
   * Offers information on whether the extraction will preserve the original ordering of the values.
   * <p>
   * Some optimizations of queries is possible if ordering is preserved.  Null values *do* count towards
   * ordering.
   *
   * @return true if ordering is preserved, false otherwise
   */
  boolean preservesOrdering();

  /**
   * A dim extraction can be of one of two types, renaming or rebucketing. In the `ONE_TO_ONE` case, a unique values is
   * modified into another unique value. In the `MANY_TO_ONE` case, there is no longer a 1:1 relation between old dimension
   * value and new dimension value
   *
   * @return {@link ExtractionFn.ExtractionType} declaring what kind of manipulation this function does
   */
  ExtractionType getExtractionType();

  enum ExtractionType
  {
    MANY_TO_ONE, ONE_TO_ONE
  }
}
