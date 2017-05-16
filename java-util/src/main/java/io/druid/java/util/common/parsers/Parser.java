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

package io.druid.java.util.common.parsers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Class that can parse Strings into Maps.
 */
public interface Parser<K, V>
{
  /**
   * This method may or may not get called at the start of reading of every file depending on the type of IndexTasks.
   * The parser state should be reset if exists.
   */
  default void startFileFromBeginning()
  {

  }

  /**
   * Parse a String into a Map.  The result can be null which means the given input string will be ignored.
   *
   * @throws ParseException if the String cannot be parsed
   */
  @Nullable
  Map<K, V> parse(String input);

  /**
   * Set the fieldNames that you expect to see in parsed Maps. Deprecated; Parsers should not, in general, be
   * expected to know what fields they will return. Some individual types of parsers do need to know (like a TSV
   * parser) and those parsers have their own way of setting field names.
   */
  @Deprecated
  void setFieldNames(Iterable<String> fieldNames);

  /**
   * Returns the fieldNames that we expect to see in parsed Maps, if known, or null otherwise. Deprecated; Parsers
   * should not, in general, be expected to know what fields they will return.
   */
  @Deprecated
  List<String> getFieldNames();
}
