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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Caches {@link LookupExtractor} within a {@link PlannerContext}.
 */
public class PlannerLookupCache
{
  @Nullable
  private final LookupExtractorFactoryContainerProvider lookupProvider;
  private final Map<String, LookupExtractor> map;

  PlannerLookupCache(@Nullable final LookupExtractorFactoryContainerProvider lookupProvider)
  {
    this.lookupProvider = lookupProvider;
    this.map = new HashMap<>();
  }

  @Nullable
  LookupExtractor getLookup(final String lookupName)
  {
    if (lookupProvider == null) {
      return null;
    }

    return map.computeIfAbsent(
        lookupName,
        name -> {
          final Optional<LookupExtractorFactoryContainer> maybeContainer = lookupProvider.get(name);
          return maybeContainer.map(container -> container.getLookupExtractorFactory().get())
                               .orElse(null);
        }
    );
  }
}
