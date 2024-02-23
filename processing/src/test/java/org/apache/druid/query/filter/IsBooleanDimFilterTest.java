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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class IsBooleanDimFilterTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EqualityFilter baseFilter = new EqualityFilter("x", ColumnType.STRING, "hello", null);

    IsTrueDimFilter trueFilter = IsTrueDimFilter.of(baseFilter);
    String s = mapper.writeValueAsString(trueFilter);
    Assert.assertEquals(trueFilter, mapper.readValue(s, IsTrueDimFilter.class));

    IsFalseDimFilter falseFilter = IsFalseDimFilter.of(baseFilter);
    s = mapper.writeValueAsString(falseFilter);
    Assert.assertEquals(falseFilter, mapper.readValue(s, IsFalseDimFilter.class));

  }

  @Test
  public void testGetCacheKey()
  {
    IsTrueDimFilter f1 = IsTrueDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "hello", null));
    IsTrueDimFilter f1_2 = IsTrueDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "hello", null));
    IsFalseDimFilter f1_3 = IsFalseDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "hello", null));
    IsFalseDimFilter f1_4 = IsFalseDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "hello", null));
    IsTrueDimFilter f2 = IsTrueDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "world", null));
    IsTrueDimFilter f3 = IsTrueDimFilter.of(new EqualityFilter("x", ColumnType.STRING, "hello", new FilterTuning(true, null, null)));
    Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
    Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f1_3.getCacheKey()));
    Assert.assertArrayEquals(f1_3.getCacheKey(), f1_4.getCacheKey());
    Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
    Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

  }

  @Test
  public void testInvalidParameters()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> new IsTrueDimFilter(null)
    );
    Assert.assertEquals("IS TRUE operator requires a non-null filter for field", t.getMessage());
    t = Assert.assertThrows(
        DruidException.class,
        () -> new IsFalseDimFilter(null)
    );
    Assert.assertEquals("IS FALSE operator requires a non-null filter for field", t.getMessage());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(IsTrueDimFilter.class).usingGetClass()
                  .withNonnullFields("field")
                  .withIgnoredFields("optimizedFilterIncludeUnknown", "optimizedFilterNoIncludeUnknown")
                  .verify();

    EqualsVerifier.forClass(IsFalseDimFilter.class).usingGetClass()
                  .withNonnullFields("field")
                  .withIgnoredFields("optimizedFilterIncludeUnknown", "optimizedFilterNoIncludeUnknown")
                  .verify();
  }
}
