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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class RestrictedInputNumberDataSourceTest
{
  private final RestrictedInputNumberDataSource restrictedFooDataSource = new RestrictedInputNumberDataSource(
      0,
      RowFilterPolicy.from(TrueDimFilter.instance())
  );
  private final RestrictedInputNumberDataSource restrictedBarDataSource = new RestrictedInputNumberDataSource(
      1,
      NoRestrictionPolicy.instance()
  );

  @Test
  public void test_creation_failWithNullPolicy()
  {
    Exception e = Assert.assertThrows(Exception.class, () -> new RestrictedInputNumberDataSource(1, null));
    Assert.assertEquals(e.getMessage(), "Policy can't be null");
  }

  @Test
  public void test_getTableNames()
  {
    Assert.assertTrue(restrictedFooDataSource.getTableNames().isEmpty());
    Assert.assertTrue(restrictedBarDataSource.getTableNames().isEmpty());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertTrue(restrictedFooDataSource.getChildren().isEmpty());
    Assert.assertTrue(restrictedBarDataSource.getChildren().isEmpty());
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
        () -> restrictedFooDataSource.withChildren(ImmutableList.of(new TableDataSource("foo")))
    );
    Assert.assertEquals(exception.getMessage(), "Cannot accept children");

    RestrictedInputNumberDataSource newRestrictedDataSource = (RestrictedInputNumberDataSource) restrictedFooDataSource.withChildren(
        ImmutableList.of());
    Assert.assertTrue(newRestrictedDataSource.getChildren().isEmpty());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(RestrictedInputNumberDataSource.class)
                  .usingGetClass()
                  .withNonnullFields("inputNumber", "policy")
                  .verify();
  }

  @Test
  public void test_deserialize_fromObject() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final RestrictedInputNumberDataSource deserializedRestrictedDataSource = jsonMapper.readValue(
        "{\"type\":\"restrictedInputNumber\",\"inputNumber\":1,\"policy\":{\"type\":\"noRestriction\"}}\n",
        RestrictedInputNumberDataSource.class
    );

    Assert.assertEquals(
        deserializedRestrictedDataSource,
        restrictedBarDataSource
    );
  }

  @Test
  public void test_serialize() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String s = jsonMapper.writeValueAsString(restrictedFooDataSource);

    Assert.assertEquals(
        "{\"type\":\"restrictedInputNumber\",\"inputNumber\":0,\"policy\":{\"type\":\"row\",\"rowFilter\":{\"type\":\"true\"}}}",
        s
    );
  }

  @Test
  public void testStringRep()
  {
    Assert.assertNotEquals(restrictedFooDataSource.toString(), restrictedBarDataSource.toString());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    final RestrictedInputNumberDataSource dataSource = new RestrictedInputNumberDataSource(
        0,
        RowFilterPolicy.from(TrueDimFilter.instance())
    );

    Assert.assertEquals(
        dataSource,
        mapper.readValue(mapper.writeValueAsString(dataSource), DataSource.class)
    );
  }
}
