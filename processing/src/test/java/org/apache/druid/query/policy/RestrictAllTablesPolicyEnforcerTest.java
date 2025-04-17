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

package org.apache.druid.query.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestSegmentUtils.SegmentForTesting;
import org.junit.Assert;
import org.junit.Test;

public class RestrictAllTablesPolicyEnforcerTest
{
  @Test
  public void test_serialize() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(
        NoRestrictionPolicy.class.getName()));
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    String expected = "{\"type\":\"restrictAllTables\",\"allowedPolicies\":[\"org.apache.druid.query.policy.NoRestrictionPolicy\"]}";
    Assert.assertEquals(expected, jsonMapper.writeValueAsString(policyEnforcer));
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    PolicyEnforcer deserialized = jsonMapper.readValue(
        jsonMapper.writeValueAsString(policyEnforcer),
        PolicyEnforcer.class
    );
    Assert.assertEquals(policyEnforcer, deserialized);
  }

  @Test
  public void test_validate() throws Exception
  {
    final RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(null);
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("some-col", null));
    TableDataSource table = TableDataSource.create("table");
    RestrictedDataSource restricted = RestrictedDataSource.create(table, policy);
    // Test validate data source, fail for TableDataSource, success for RestrictedDataSource
    Assert.assertFalse(policyEnforcer.validate(null));
    Assert.assertTrue(policyEnforcer.validate(policy));
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> policyEnforcer.validateOrElseThrow(table, null)
    );
    Assert.assertEquals(DruidException.Category.FORBIDDEN, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assert.assertEquals(
        "Failed security validation with dataSource [table]",
        e.getMessage()
    );
    policyEnforcer.validateOrElseThrow(restricted.getBase(), restricted.getPolicy());
    // Test validate segment, fail for ReferenceCountingSegment not wrapped with any policy, success when wrapped with a policy
    Segment baseSegment = new SegmentForTesting("table", Intervals.ETERNITY, "1");
    ReferenceCountingSegment segment = ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment);
    Assert.assertFalse(policyEnforcer.validate(null));
    Assert.assertTrue(policyEnforcer.validate(policy));
    final DruidException e2 = Assert.assertThrows(
        DruidException.class,
        () -> policyEnforcer.validateOrElseThrow(segment, null)
    );
    Assert.assertEquals(DruidException.Category.FORBIDDEN, e2.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e2.getTargetPersona());
    Assert.assertEquals(
        "Failed security validation with segment [table_-146136543-09-08T08:23:32.096Z_146140482-04-24T15:36:27.903Z_1]",
        e2.getMessage()
    );
    policyEnforcer.validateOrElseThrow(segment, policy);
    // Test multiple layers of ReferenceCountingSegment, success:
    //    Restrict
    //        Ref
    //           Ref_base
    //        policy
    ReferenceCountingSegment wrapSegment = ReferenceCountingSegment.wrapRootGenerationSegment(segment);
    policyEnforcer.validateOrElseThrow(wrapSegment, policy);
    // Test multiple layers of ReferenceCountingSegment, fail:
    //    Ref
    //        Ref
    //            Ref_base
    Exception e3 = Assert.assertThrows(
        Exception.class,
        () -> policyEnforcer.validateOrElseThrow(wrapSegment, null)
    );
    Assert.assertEquals(
        "Failed security validation with segment [table_-146136543-09-08T08:23:32.096Z_146140482-04-24T15:36:27.903Z_1]",
        e3.getMessage()
    );
    // Test multiple layers of ReferenceCountingSegment, success:
    //    Ref
    //        Restrict
    //            Ref_base
    //            policy
    RestrictedSegment restrictedSegment = new RestrictedSegment(segment, policy);
    ReferenceCountingSegment wrapRestrictedSegment = ReferenceCountingSegment.wrapRootGenerationSegment(
        restrictedSegment);
    policyEnforcer.validateOrElseThrow(wrapRestrictedSegment, null);
    // Test multiple layers of ReferenceCountingSegment, fail:
    //    Restrict
    //        Restrict
    //            Ref_base
    //            policy
    //        policy
    Exception e4 = Assert.assertThrows(
        Exception.class,
        () -> policyEnforcer.validateOrElseThrow(wrapRestrictedSegment, policy)
    );
    Assert.assertEquals("policy wrapped with another policy is not allowed", e4.getMessage());
  }

  @Test
  public void test_validate_withAllowedPolicies() throws Exception
  {
    RestrictAllTablesPolicyEnforcer policyEnforcer = new RestrictAllTablesPolicyEnforcer(ImmutableList.of(
        NoRestrictionPolicy.class.getName()));
    RowFilterPolicy policy = RowFilterPolicy.from(new NullFilter("some-col", null));
    TableDataSource table = TableDataSource.create("table");
    RestrictedDataSource restricted = RestrictedDataSource.create(table, policy);

    Assert.assertThrows(DruidException.class, () -> policyEnforcer.validateOrElseThrow(table, null));
    Assert.assertThrows(
        DruidException.class,
        () -> policyEnforcer.validateOrElseThrow(restricted.getBase(), restricted.getPolicy())
    );
    policyEnforcer.validateOrElseThrow(table, NoRestrictionPolicy.instance());

    Segment baseSegment = new SegmentForTesting("table", Intervals.ETERNITY, "1");
    ReferenceCountingSegment segment = ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment);
    Assert.assertThrows(DruidException.class, () -> policyEnforcer.validateOrElseThrow(segment, null));
    Assert.assertThrows(DruidException.class, () -> policyEnforcer.validateOrElseThrow(segment, policy));
    policyEnforcer.validateOrElseThrow(segment, NoRestrictionPolicy.instance());
  }
}
