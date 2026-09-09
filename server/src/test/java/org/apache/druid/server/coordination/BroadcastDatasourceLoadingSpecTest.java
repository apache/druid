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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Set;

public class BroadcastDatasourceLoadingSpecTest
{
  @Test
  public void testLoadingAllBroadcastDatasources()
  {
    final BroadcastDatasourceLoadingSpec spec = BroadcastDatasourceLoadingSpec.ALL;
    Assertions.assertEquals(BroadcastDatasourceLoadingSpec.Mode.ALL, spec.getMode());
    Assertions.assertNull(spec.getBroadcastDatasourcesToLoad());
  }

  @Test
  public void testModeNeedsBroadcastSegments()
  {
    Assertions.assertTrue(BroadcastDatasourceLoadingSpec.Mode.ALL.needsBroadcastSegments());
    Assertions.assertFalse(BroadcastDatasourceLoadingSpec.Mode.NONE.needsBroadcastSegments());
    Assertions.assertTrue(BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED.needsBroadcastSegments());
  }

  @Test
  public void testLoadingNoLookups()
  {
    final BroadcastDatasourceLoadingSpec spec = BroadcastDatasourceLoadingSpec.NONE;
    Assertions.assertEquals(BroadcastDatasourceLoadingSpec.Mode.NONE, spec.getMode());
    Assertions.assertNull(spec.getBroadcastDatasourcesToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookups()
  {
    final Set<String> broadcastDatasourcesToLoad = ImmutableSet.of("ds1", "ds2");
    final BroadcastDatasourceLoadingSpec spec = BroadcastDatasourceLoadingSpec.loadOnly(ImmutableSet.of("ds1", "ds2"));
    Assertions.assertEquals(BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED, spec.getMode());
    Assertions.assertEquals(broadcastDatasourcesToLoad, spec.getBroadcastDatasourcesToLoad());
  }

  @Test
  public void testLoadingOnlyRequiredLookupsWithNullList()
  {
    DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> BroadcastDatasourceLoadingSpec.loadOnly(null)
    );
    Assertions.assertEquals("Expected non-null set of broadcast datasources to load.", exception.getMessage());
  }

  @Test
  public void testCreateBroadcastLoadingSpecFromNullContext()
  {
    // Default spec is returned in the case of context=null.
    Assertions.assertEquals(
        BroadcastDatasourceLoadingSpec.NONE,
        BroadcastDatasourceLoadingSpec.createFromContext(
            null,
            BroadcastDatasourceLoadingSpec.NONE
        )
    );

    Assertions.assertEquals(
        BroadcastDatasourceLoadingSpec.ALL,
        BroadcastDatasourceLoadingSpec.createFromContext(
            null,
            BroadcastDatasourceLoadingSpec.ALL
        )
    );
  }

  @Test
  public void testCreateBroadcastLoadingSpecFromContext()
  {
    // Only required lookups are returned in the case of context having the lookup keys.
    Assertions.assertEquals(
        BroadcastDatasourceLoadingSpec.loadOnly(ImmutableSet.of("ds1", "ds2")),
        BroadcastDatasourceLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD,
                Arrays.asList("ds1", "ds2"),
                BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE,
                BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED
            ),
            BroadcastDatasourceLoadingSpec.ALL
        )
    );

    // No lookups are returned in the case of context having mode=NONE, irrespective of the default spec.
    Assertions.assertEquals(
        BroadcastDatasourceLoadingSpec.NONE,
        BroadcastDatasourceLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE,
                BroadcastDatasourceLoadingSpec.Mode.NONE
            ),
            BroadcastDatasourceLoadingSpec.ALL
        )
    );

    // All lookups are returned in the case of context having mode=ALL, irrespective of the default spec.
    Assertions.assertEquals(
        BroadcastDatasourceLoadingSpec.ALL,
        BroadcastDatasourceLoadingSpec.createFromContext(
            ImmutableMap.of(
                BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE,
                BroadcastDatasourceLoadingSpec.Mode.ALL
            ),
            BroadcastDatasourceLoadingSpec.NONE
        )
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "NONE1",
      "A",
      "Random mode",
      "all",
      "only required",
      "none"
  })
  public void testSpecFromInvalidModeInContext(final String mode)
  {
    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> BroadcastDatasourceLoadingSpec.createFromContext(
            ImmutableMap.of(BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE, mode),
            BroadcastDatasourceLoadingSpec.ALL
        )
    );
    Assertions.assertEquals(
        StringUtils.format(
            "Invalid value of %s[%s]. Allowed values are [ALL, NONE, ONLY_REQUIRED]",
            BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE, mode
        ),
        exception.getMessage()
    );
  }


  @ParameterizedTest
  @ValueSource(strings = {
      "foo bar",
      "foo]"
  })
  public void testSpecFromInvalidBroadcastDatasourcesInContext(final Object lookupsToLoad)
  {
    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () ->
            BroadcastDatasourceLoadingSpec.createFromContext(
                ImmutableMap.of(
                    BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD,
                    lookupsToLoad,
                    BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCE_LOADING_MODE,
                    BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED
                ),
                BroadcastDatasourceLoadingSpec.ALL
            )
    );
    Assertions.assertEquals(
        StringUtils.format(
            "Invalid value of %s[%s]. Please provide a comma-separated list of "
            + "broadcast datasource names. For example: [\"datasourceName1\", \"datasourceName2\"]",
            BroadcastDatasourceLoadingSpec.CTX_BROADCAST_DATASOURCES_TO_LOAD, lookupsToLoad
        ),
        exception.getMessage()
    );
  }
}
