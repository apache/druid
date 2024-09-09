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

package org.apache.druid.server.coordination;

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
public class BroadcastLoadingSpecTest
{
  @Test
  public void testLoadingAllBroadcastDatasources()
  {
    final BroadcastLoadingSpec spec = BroadcastLoadingSpec.ALL;
    Assert.assertEquals(BroadcastLoadingSpec.Mode.ALL, spec.getMode());
    Assert.assertNull(spec.getbroadcastDatasourcesToLoad());
  }

  @Test
  public void testLoadingNoLookups()
  {
    final BroadcastLoadingSpec spec = BroadcastLoadingSpec.NONE;
    Assert.assertEquals(BroadcastLoadingSpec.Mode.NONE, spec.getMode());
    Assert.assertNull(spec.getbroadcastDatasourcesToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookups()
  {
    final Set<String> broadcastDatasourcesToLoad = ImmutableSet.of("ds1", "ds2");
    final BroadcastLoadingSpec spec = BroadcastLoadingSpec.loadOnly(ImmutableSet.of("ds1", "ds2"));
    Assert.assertEquals(BroadcastLoadingSpec.Mode.ONLY_REQUIRED, spec.getMode());
    Assert.assertEquals(broadcastDatasourcesToLoad, spec.getbroadcastDatasourcesToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookupsWithNullList()
  {
    DruidException exception = Assert.assertThrows(DruidException.class, () -> BroadcastLoadingSpec.loadOnly(null));
    Assert.assertEquals("Expected non-null set of broadcastDatasources to load.", exception.getMessage());
  }

  @Test
  public void testCreateBroadcastLoadingSpecFromNullContext()
  {
    // Default spec is returned in the case of context=null.
    Assert.assertEquals(
        BroadcastLoadingSpec.NONE,
        BroadcastLoadingSpec.createFromContext(
            null,
            BroadcastLoadingSpec.NONE
        )
    );

    Assert.assertEquals(
        BroadcastLoadingSpec.ALL,
        BroadcastLoadingSpec.createFromContext(
            null,
            BroadcastLoadingSpec.ALL
        )
    );
  }

  @Test
  public void testCreateBroadcastLoadingSpecFromContext()
  {
    // Only required lookups are returned in the case of context having the lookup keys.
    Assert.assertEquals(
        BroadcastLoadingSpec.loadOnly(ImmutableSet.of("ds1", "ds2")),
        BroadcastLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD, Arrays.asList("ds1", "ds2"),
                BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, BroadcastLoadingSpec.Mode.ONLY_REQUIRED
            ),
            BroadcastLoadingSpec.ALL
        )
    );

    // No lookups are returned in the case of context having mode=NONE, irrespective of the default spec.
    Assert.assertEquals(
        BroadcastLoadingSpec.NONE,
        BroadcastLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, BroadcastLoadingSpec.Mode.NONE),
            BroadcastLoadingSpec.ALL
        )
    );

    // All lookups are returned in the case of context having mode=ALL, irrespective of the default spec.
    Assert.assertEquals(
        BroadcastLoadingSpec.ALL,
        BroadcastLoadingSpec.createFromContext(
            ImmutableMap.of(BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, BroadcastLoadingSpec.Mode.ALL),
            BroadcastLoadingSpec.NONE
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
  public void testCreateLookupLoadingSpecFromInvalidModeInContext(final String mode)
  {
    final DruidException exception = Assert.assertThrows(DruidException.class, () -> BroadcastLoadingSpec.createFromContext(
        ImmutableMap.of(BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, mode), BroadcastLoadingSpec.ALL));
    Assert.assertEquals(StringUtils.format("Invalid value of %s[%s]. Allowed values are [ALL, NONE, ONLY_REQUIRED]",
                                           BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, mode), exception.getMessage());
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
        BroadcastLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD, lookupsToLoad,
                BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_LOADING_MODE, BroadcastLoadingSpec.Mode.ONLY_REQUIRED),
            BroadcastLoadingSpec.ALL)
    );
    Assert.assertEquals(StringUtils.format("Invalid value of %s[%s]. Please provide a comma-separated list of "
                                           + "broadcastDatasource names. For example: [\"lookupName1\", \"lookupName2\"]",
                                           BroadcastLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD, lookupsToLoad), exception.getMessage());
  }
}