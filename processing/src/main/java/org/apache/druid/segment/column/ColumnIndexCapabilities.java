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

package org.apache.druid.segment.column;

import org.apache.druid.query.filter.ColumnIndexSelector;

/**
 * Sort of like {@link ColumnCapabilities}, except for indexes supplied by {@link ColumnIndexSelector}, provides
 * information for how query processing may use indexes.
 */
public interface ColumnIndexCapabilities
{
  /**
   * Indicates if an index can be inverted for use with a 'NOT' filter. Some types of indexes may not be invertible,
   * such as those which provide false positive matches.
   */
  boolean isInvertible();

  /**
   * Indicates if an index is an exact match, or should also be post-filtered with a value matcher. Filters which
   * are not an exact match must always use a value matcher as a post-filter, even if they have an index.
   */
  boolean isExact();

  ColumnIndexCapabilities merge(ColumnIndexCapabilities other);
}
