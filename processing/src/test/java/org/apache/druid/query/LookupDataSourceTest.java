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
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class LookupDataSourceTest
{
  private final LookupDataSource lookylooDataSource = new LookupDataSource("lookyloo");

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(Collections.emptySet(), lookylooDataSource.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(Collections.emptyList(), lookylooDataSource.getChildren());
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertFalse(lookylooDataSource.isCacheable(true));
    Assertions.assertFalse(lookylooDataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertTrue(lookylooDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(lookylooDataSource.isProcessable());
  }

  @Test
  public void test_withChildren_empty()
  {
    Assertions.assertSame(lookylooDataSource, lookylooDataSource.withChildren(Collections.emptyList()));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> lookylooDataSource.withChildren(ImmutableList.of(new LookupDataSource("bar")))
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot accept children"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(LookupDataSource.class).usingGetClass().withNonnullFields("lookupName").verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final LookupDataSource deserialized = (LookupDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(lookylooDataSource),
        DataSource.class
    );

    Assertions.assertEquals(lookylooDataSource, deserialized);
  }
}
