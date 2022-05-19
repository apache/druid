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

package org.apache.druid.query.lookup;

import java.util.Optional;
import java.util.Set;

/**
 * Provides {@link LookupExtractorFactoryContainer} to query and indexing time dimension transformations.
 *
 * The most important production implementation is LookupReferencesManager.
 */
public interface LookupExtractorFactoryContainerProvider
{
  /**
   * Returns the set of all lookup names that {@link #get} can return containers for. Note that because the underlying
   * set of valid lookups might change over time, it is not guaranteed that calling {@link #get} on the results will
   * actually yield a container (it might have been removed).
   */
  Set<String> getAllLookupNames();

  /**
   * Returns a lookup container for the provided lookupName, if it exists.
   */
  Optional<LookupExtractorFactoryContainer> get(String lookupName);
}
