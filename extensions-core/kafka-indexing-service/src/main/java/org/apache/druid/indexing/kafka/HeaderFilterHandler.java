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

package org.apache.druid.indexing.kafka;

/**
 * Interface for handling different filter types in Kafka header evaluation.
 *
 * This provides an extensible way to support various Druid filter types for
 * header-based filtering without modifying the core evaluation logic.
 */
public interface HeaderFilterHandler
{
  /**
   * Gets the header name/key to evaluate from the filter.
   *
   * @return the header name to look for in Kafka message headers
   */
  String getHeaderName();

  /**
   * Evaluates whether a record should be included based on the header value.
   *
   * @param headerValue the decoded header value (guaranteed to be non-null when called)
   * @return true if the record should be included, false if it should be filtered out
   */
  boolean shouldInclude(String headerValue);

  /**
   * Gets a human-readable description of this filter for logging and debugging.
   *
   * @return a descriptive string representation of the filter
   */
  String getDescription();

}
