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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RestrictedDataSourceTest
{
  private final TableDataSource fooDataSource = new TableDataSource("foo");
  private final TableDataSource barDataSource = new TableDataSource("bar");
  private final RestrictedDataSource restrictedFooDataSource = RestrictedDataSource.create(
      fooDataSource,
      RowFilterPolicy.from(TrueDimFilter.instance())
  );
  private final RestrictedDataSource restrictedBarDataSource = RestrictedDataSource.create(
      barDataSource,
      NoRestrictionPolicy.instance()
  );

  @Test
  public void test_creation_failWithNullPolicy()
  {
    IAE e = Assert.assertThrows(IAE.class, () -> RestrictedDataSource.create(fooDataSource, null));
    Assert.assertEquals(e.getMessage(), "Policy can't be null for RestrictedDataSource");
  }

  @Test
  public void test_getTableNames()
  {
    Assert.assertEquals(Collections.singleton("foo"), restrictedFooDataSource.getTableNames());
    Assert.assertEquals(Collections.singleton("bar"), restrictedBarDataSource.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertEquals(Collections.singletonList(fooDataSource), restrictedFooDataSource.getChildren());
    Assert.assertEquals(Collections.singletonList(barDataSource), restrictedBarDataSource.getChildren());
  }

  @Test
  public void test_isCacheable()
  {
    Assert.assertFalse(restrictedFooDataSource.isCacheable(true));
  }

  @Test
  public void test_isGlobal()
  {
    Assert.assertFalse(restrictedFooDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assert.assertTrue(restrictedFooDataSource.isProcessable());
  }

  @Test
  public void test_withChildren()
  {
    IllegalArgumentException exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> restrictedFooDataSource.withChildren(Collections.emptyList())
    );
    Assert.assertEquals(exception.getMessage(), "Expected [1] child, got [0]");

    IllegalArgumentException exception2 = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> restrictedFooDataSource.withChildren(ImmutableList.of(fooDataSource, barDataSource))
    );
    Assert.assertEquals(exception2.getMessage(), "Expected [1] child, got [2]");

    RestrictedDataSource newRestrictedDataSource = (RestrictedDataSource) restrictedFooDataSource.withChildren(
        ImmutableList.of(barDataSource));
    Assert.assertEquals(newRestrictedDataSource.getBase(), barDataSource);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(RestrictedDataSource.class).usingGetClass().withNonnullFields("base", "policy").verify();
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final RestrictedDataSource deserialized = (RestrictedDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(restrictedFooDataSource),
        DataSource.class
    );

    Assert.assertEquals(restrictedFooDataSource, deserialized);
  }

  @Test
  public void test_deserialize_fromObject() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final RestrictedDataSource deserializedRestrictedDataSource = jsonMapper.readValue(
        "{\"type\":\"restrict\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"policy\":{\"type\":\"noRestriction\"}}",
        RestrictedDataSource.class
    );

    Assert.assertEquals(
        deserializedRestrictedDataSource,
        RestrictedDataSource.create(fooDataSource, NoRestrictionPolicy.instance())
    );
  }


  @Test
  public void test_serialize() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String s = jsonMapper.writeValueAsString(restrictedFooDataSource);

    Assert.assertEquals(
        "{\"type\":\"restrict\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"policy\":{\"type\":\"row\",\"rowFilter\":{\"type\":\"true\"}}}",
        s
    );
  }

  @Test
  public void testStringRep()
  {
    Assert.assertNotEquals(restrictedFooDataSource.toString(), restrictedBarDataSource.toString());
  }
}
