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

package org.apache.druid.server.lookup.cache;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class LookupLoadingSpecTest
{
  @Test
  public void testLoadingAllLookups()
  {
    LookupLoadingSpec spec = LookupLoadingSpec.ALL;
    Assert.assertEquals(LookupLoadingSpec.Mode.ALL, spec.getMode());
    Assert.assertNull(spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingNoLookups()
  {
    LookupLoadingSpec spec = LookupLoadingSpec.NONE;
    Assert.assertEquals(LookupLoadingSpec.Mode.NONE, spec.getMode());
    Assert.assertNull(spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookups()
  {
    Set<String> lookupsToLoad = ImmutableSet.of("lookupName1", "lookupName2");
    LookupLoadingSpec spec = LookupLoadingSpec.loadOnly(lookupsToLoad);
    Assert.assertEquals(LookupLoadingSpec.Mode.ONLY_REQUIRED, spec.getMode());
    Assert.assertEquals(lookupsToLoad, spec.getLookupsToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookupsWithNullList()
  {
    DruidException exception = Assert.assertThrows(DruidException.class, () -> LookupLoadingSpec.loadOnly(null));
    Assert.assertEquals("Expected non-null set of lookups to load.", exception.getMessage());
  }

  @MethodSource("provideParamsForTestGetSpecFromContext")
  @ParameterizedTest
  public void testGetLookupLoadingSpecFromContext(Map<String, Object> context, LookupLoadingSpec defaultSpec, LookupLoadingSpec expectedSpec)
  {
    LookupLoadingSpec specFromContext = LookupLoadingSpec.getSpecFromContext(context, defaultSpec);
    Assert.assertEquals(expectedSpec.getMode(), specFromContext.getMode());
    Assert.assertEquals(expectedSpec.getLookupsToLoad(), specFromContext.getLookupsToLoad());
  }

  public static Collection<Object[]> provideParamsForTestGetSpecFromContext()
  {
    ImmutableSet<String> lookupsToLoad = ImmutableSet.of("lookupName1", "lookupName2");
    final ImmutableMap<String, Object> contextWithModeOnlyRequired = ImmutableMap.of(
        LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, new ArrayList<>(lookupsToLoad),
        LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED);
    final ImmutableMap<String, Object> contextWithModeNone = ImmutableMap.of(
        LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.NONE);
    final ImmutableMap<String, Object> contextWithModeAll = ImmutableMap.of(
        LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ALL);
    final ImmutableMap<String, Object> contextWithoutLookupKeys = ImmutableMap.of();

    // Return params: <context, default LookupLoadingSpec, expected LookupLoadingSpec>
    Object[][] params = new Object[][]{
        // Default spec is returned in the case of context not having the lookup keys.
        {
            contextWithoutLookupKeys,
            LookupLoadingSpec.ALL,
            LookupLoadingSpec.ALL
        },
        // Default spec is returned in the case of context not having the lookup keys.
        {
            contextWithoutLookupKeys,
            LookupLoadingSpec.NONE,
            LookupLoadingSpec.NONE
        },
        // Only required lookups are returned in the case of context having the lookup keys.
        {
            contextWithModeOnlyRequired,
            LookupLoadingSpec.ALL,
            LookupLoadingSpec.loadOnly(lookupsToLoad)
        },
        // No lookups are returned in the case of context having mode=NONE, irrespective of the default spec.
        {
            contextWithModeAll,
            LookupLoadingSpec.NONE,
            LookupLoadingSpec.ALL
        },
        // All lookups are returned in the case of context having mode=ALL, irrespective of the default spec.
        {
            contextWithModeNone,
            LookupLoadingSpec.ALL,
            LookupLoadingSpec.NONE
        }
    };

    return Arrays.asList(params);
  }
}
