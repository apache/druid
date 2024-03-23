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

package org.apache.druid.sql.calcite.util;

import com.google.inject.Inject;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestLookupProvider implements LookupExtractorFactoryContainerProvider
{
  private final Map<String, LookupExtractor> lookupMap;

  @Inject
  public TestLookupProvider(Map<String, LookupExtractor> lookupMap)
  {
    this.lookupMap = lookupMap;
  }

  @Override
  public Set<String> getAllLookupNames()
  {
    return lookupMap.keySet();
  }

  @Override
  public Optional<LookupExtractorFactoryContainer> get(String lookupName)
  {
    final LookupExtractor theLookup = lookupMap.get(lookupName);
    if (theLookup != null) {
      return Optional.of(new LookupEnabledTestExprMacroTable.TestLookupContainer(theLookup));
    } else {
      return Optional.empty();
    }
  }
}
