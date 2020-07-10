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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Base class for DimFilters that support optimization. This abstract class provides a default implementation of
 * toOptimizedFilter that relies on the existing optimize() and toFilter() methods. It uses a memoized supplier.
 */
public abstract class AbstractOptimizableDimFilter implements DimFilter
{
  private final Supplier<Filter> cachedOptimizedFilter = Suppliers.memoize(
      () -> optimize().toFilter()
  );

  @JsonIgnore
  @Override
  public Filter toOptimizedFilter()
  {
    return cachedOptimizedFilter.get();
  }
}
