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

import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;

/**
 * Factory for creating HeaderFilterHandler instances.
 *
 * This factory uses explicit instanceof checks for clarity and performance,
 * making it easy to add support for new filter types by simply adding
 * new conditional branches.
 */
public final class HeaderFilterHandlerFactory
{
  private HeaderFilterHandlerFactory()
  {
    // Utility class - prevent instantiation
  }

  /**
   * Creates the appropriate handler for the given filter.
   *
   * @param filter the Druid filter to create a handler for
   * @return a HeaderFilterHandler that can evaluate the given filter type
   * @throws IllegalArgumentException if the filter type is not supported
   */
  public static HeaderFilterHandler forFilter(Filter filter)
  {
    if (filter instanceof InDimFilter) {
      return new InDimFilterHandler((InDimFilter) filter);
    }

    throw new IllegalArgumentException(
        "Unsupported filter type for header filtering: " + filter.getClass().getSimpleName() +
        ". Supported types: InDimFilter"
    );
  }
}
