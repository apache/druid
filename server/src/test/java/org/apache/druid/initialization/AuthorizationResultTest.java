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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.AuthorizationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class AuthorizationResultTest
{
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
            Optional.of(NoRestrictionPolicy.instance()),
            "table2",
            Optional.of(
                RowFilterPolicy.from(new EqualityFilter("column1", ColumnType.STRING, "val1", null)))
        )
    );
    Assertions.assertEquals(
        "AuthorizationResult [permission=ALLOW_WITH_RESTRICTION, failureMessage=null, policyRestrictions={table1=Optional[NO_RESTRICTION], table2=Optional[RowFilterPolicy{rowFilter=column1 = val1}]}]",
        result.toString()
    );
  }

  @Test
  public void testNoAccess()
  {
    AuthorizationResult result = AuthorizationResult.deny("this data source is not permitted");
    Assertions.assertFalse(result.allowBasicAccess());
    Assertions.assertFalse(result.allowAccessWithNoRestriction());
    Assertions.assertEquals("this data source is not permitted", result.getErrorMessage());
    Assertions.assertFalse(result.allowAccessWithNoRestriction());
  }

  @Test
  public void testFullAccess()
  {
    AuthorizationResult result = AuthorizationResult.allowWithRestriction(ImmutableMap.of());
    Assertions.assertTrue(result.allowBasicAccess());
    Assertions.assertTrue(result.allowAccessWithNoRestriction());
    Assertions.assertThrows(DruidException.class, result::getErrorMessage);

    AuthorizationResult resultWithEmptyPolicy = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.empty()
    ));
    Assertions.assertTrue(resultWithEmptyPolicy.allowBasicAccess());
    Assertions.assertTrue(resultWithEmptyPolicy.allowAccessWithNoRestriction());
    Assertions.assertThrows(DruidException.class, resultWithEmptyPolicy::getErrorMessage);

    AuthorizationResult resultWithNoRestrictionPolicy = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.of(NoRestrictionPolicy.instance())
    ));
    Assertions.assertTrue(resultWithNoRestrictionPolicy.allowBasicAccess());
    Assertions.assertTrue(resultWithNoRestrictionPolicy.allowAccessWithNoRestriction());
    Assertions.assertThrows(DruidException.class, resultWithNoRestrictionPolicy::getErrorMessage);
  }

  @Test
  public void testRestrictedAccess()
  {
    AuthorizationResult result = AuthorizationResult.allowWithRestriction(ImmutableMap.of(
        "table1",
        Optional.of(RowFilterPolicy.from(new EqualityFilter(
            "col",
            ColumnType.STRING,
            "val1",
            null
        )))
    ));
    Assertions.assertTrue(result.allowBasicAccess());
    Assertions.assertFalse(result.allowAccessWithNoRestriction());
    Assertions.assertEquals("Unauthorized", result.getErrorMessage());
  }
}
