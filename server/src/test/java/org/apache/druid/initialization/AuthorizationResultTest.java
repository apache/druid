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

package org.apache.druid.initialization;

import com.google.common.collect.ImmutableMap;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.AuthorizationResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(JUnitParamsRunner.class)
public class AuthorizationResultTest
{
  @Before
  public void setUp()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(AuthorizationResult.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testToString()
  {
    AuthorizationResult result = AuthorizationResult.allowWithRestriction(
        ImmutableMap.of(
            "table1",
            Optional.of(Policy.NO_RESTRICTION),
            "table2",
            Optional.of(
                Policy.fromRowFilter(new EqualityFilter("column1", ColumnType.STRING, "val1", null)))
        )
    );
    assertEquals(
        "AuthorizationResult [permission=ALLOW_WITH_RESTRICTION, failureMessage=null, policyRestrictions={table1=Optional[Policy{rowFilter=null}], table2=Optional[Policy{rowFilter=column1 = val1}]}, sqlResourceActions=null, allResourceActions=null]",
        result.toString()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void testFailedAccess_withPermissionError(boolean policyRestrictionsNotPermitted)
  {
    AuthorizationResult result = AuthorizationResult.deny("this data source is not permitted");
    assertEquals(
        Optional.of("this data source is not permitted"),
        result.getPermissionErrorMessage(policyRestrictionsNotPermitted)
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void testFullAccess_noPermissionError(boolean policyRestrictionsNotPermitted)
  {
    AuthorizationResult result = AuthorizationResult.allowWithRestriction(ImmutableMap.of());
    AuthorizationResult resultWithEmptyPolicy = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.empty()
    ));
    AuthorizationResult resultWithNoRestrictionPolicy = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.of(Policy.NO_RESTRICTION)
    ));
    AuthorizationResult resultWithTrueFilterRestrictionPolicy = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.of(Policy.fromRowFilter(TrueDimFilter.instance()))
    ));

    assertEquals(Optional.empty(), result.getPermissionErrorMessage(policyRestrictionsNotPermitted));
    assertEquals(Optional.empty(), resultWithEmptyPolicy.getPermissionErrorMessage(policyRestrictionsNotPermitted));
    assertEquals(
        Optional.empty(),
        resultWithNoRestrictionPolicy.getPermissionErrorMessage(policyRestrictionsNotPermitted)
    );
    assertEquals(
        Optional.empty(),
        resultWithTrueFilterRestrictionPolicy.getPermissionErrorMessage(policyRestrictionsNotPermitted)
    );
  }

  @Test
  @Parameters({
      "true, Optional[Unauthorized]",
      "false, Optional.empty"
  })
  public void testRestrictedAccess_noPermissionError(boolean policyRestrictionsNotPermitted, String error)
  {
    AuthorizationResult result = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.of(Policy.fromRowFilter(new EqualityFilter(
            "col",
            ColumnType.STRING,
            "val1",
            null
        )))
    ));
    assertEquals(error, result.getPermissionErrorMessage(policyRestrictionsNotPermitted).toString());
  }
}
