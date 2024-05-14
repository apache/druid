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
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Set;

@RunWith(JUnitParamsRunner.class)
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

  @Test
  public void testCreateLookupLoadingSpecFromEmptyContext()
  {
    // Default spec is returned in the case of context not having the lookup keys.
    Assert.assertEquals(
        LookupLoadingSpec.ALL,
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(),
            LookupLoadingSpec.ALL
        )
    );

    Assert.assertEquals(
        LookupLoadingSpec.NONE,
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(),
            LookupLoadingSpec.NONE
        )
    );
  }

  @Test
  public void testCreateLookupLoadingSpecFromNullContext()
  {
    // Default spec is returned in the case of context=null.
    Assert.assertEquals(
        LookupLoadingSpec.NONE,
        LookupLoadingSpec.createFromContext(
            null,
            LookupLoadingSpec.NONE
        )
    );

    Assert.assertEquals(
        LookupLoadingSpec.ALL,
        LookupLoadingSpec.createFromContext(
            null,
            LookupLoadingSpec.ALL
        )
    );
  }

  @Test
  public void testCreateLookupLoadingSpecFromContext()
  {
    // Only required lookups are returned in the case of context having the lookup keys.
    Assert.assertEquals(
        LookupLoadingSpec.loadOnly(ImmutableSet.of("lookup1", "lookup2")),
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(
                LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, Arrays.asList("lookup1", "lookup2"),
                LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED
            ),
            LookupLoadingSpec.ALL
        )
    );

    // No lookups are returned in the case of context having mode=NONE, irrespective of the default spec.
    Assert.assertEquals(
        LookupLoadingSpec.NONE,
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(
                LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.NONE),
            LookupLoadingSpec.ALL
        )
    );

    // All lookups are returned in the case of context having mode=ALL, irrespective of the default spec.
    Assert.assertEquals(
        LookupLoadingSpec.ALL,
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ALL),
            LookupLoadingSpec.NONE
        )
    );
  }

  @Test
  @Parameters(
      {
          "NONE1",
          "A",
          "Random mode",
          "all",
          "only required",
          "none"
      }
  )
  public void testCreateLookupLoadingSpecFromInvalidModeInContext(String mode)
  {
    final DruidException exception = Assert.assertThrows(DruidException.class, () -> LookupLoadingSpec.createFromContext(
        ImmutableMap.of(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, mode), LookupLoadingSpec.ALL));
    Assert.assertEquals(StringUtils.format("Invalid value of %s[%s]. Allowed values are [ALL, NONE, ONLY_REQUIRED]",
                                           LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, mode), exception.getMessage());
  }

  @Test
  @Parameters(
      {
          "foo bar",
          "foo]"
      }
  )
  public void testCreateLookupLoadingSpecFromInvalidLookupsInContext(Object lookupsToLoad)
  {
    final DruidException exception = Assert.assertThrows(DruidException.class, () ->
        LookupLoadingSpec.createFromContext(
            ImmutableMap.of(
                LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, lookupsToLoad,
                LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED),
            LookupLoadingSpec.ALL)
    );
    Assert.assertEquals(StringUtils.format("Invalid value of %s[%s]. Please provide a comma-separated list of "
                                           + "lookup names. For example: [\"lookupName1\", \"lookupName2\"]",
                                           LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, lookupsToLoad), exception.getMessage());
  }
}
