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

package org.apache.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

public class ChecksTest
{
  private static final String NAME1 = "name1";
  private static final Integer VALUE1 = 1;
  private static final String NAME2 = "name2";
  private static final Integer VALUE2 = 2;
  private static final Integer NULL = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void checkAtMostOneNotNullFirstNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, NULL, NAME2, VALUE2);
    Assert.assertEquals(NAME2, result.getName());
    Assert.assertEquals(VALUE2, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullSecondNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, VALUE1, NAME2, NULL);
    Assert.assertEquals(NAME1, result.getName());
    Assert.assertEquals(VALUE1, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullBothNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, NULL, NAME2, NULL);
    Assert.assertEquals(NAME1, result.getName());
    Assert.assertEquals(NULL, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullNeitherNull()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        StringUtils.format(
            "[Property{name='%s', value=%s}] or [Property{name='%s', value=%s}]",
            NAME1,
            VALUE1,
            NAME2,
            VALUE2
        )
    );

    Checks.checkAtMostOneNotNull(NAME1, VALUE1, NAME2, VALUE2);
  }

  @Test
  public void testCheckOneNotNullOrEmpty()
  {
    final List<Property<Object>> properties = ImmutableList.of(
        new Property<>("p1", null),
        new Property<>("p2", 2),
        new Property<>("p3", null),
        new Property<>("p4", Collections.emptyList())
    );
    final Property<Object> property = Checks.checkOneNotNullOrEmpty(properties);
    Assert.assertEquals(new Property<>("p2", 2), property);
  }

  @Test
  public void testCheckOneNotNullOrEmptyWithTwoNonNulls()
  {
    final List<Property<Object>> properties = ImmutableList.of(
        new Property<>("p1", null),
        new Property<>("p2", 2),
        new Property<>("p3", 3),
        new Property<>("p4", Collections.emptyList())
    );
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "At most one of [Property{name='p1', value=null}, Property{name='p2', value=2}, Property{name='p3', value=3}, "
        + "Property{name='p4', value=[]}] must be present"
    );
    Checks.checkOneNotNullOrEmpty(properties);
  }

  @Test
  public void testCheckOneNotNullOrEmptyWithNonNullAndNonEmpty()
  {
    final List<Property<Object>> properties = ImmutableList.of(
        new Property<>("p1", null),
        new Property<>("p2", 2),
        new Property<>("p3", null),
        new Property<>("p4", Lists.newArrayList(1, 2))
    );
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "At most one of [Property{name='p1', value=null}, Property{name='p2', value=2}, Property{name='p3', value=null}, "
        + "Property{name='p4', value=[1, 2]}] must be present"
    );
    Checks.checkOneNotNullOrEmpty(properties);
  }

  @Test
  public void testCheckOneNotNullOrEmptyWithAllNulls()
  {
    final List<Property<Object>> properties = ImmutableList.of(
        new Property<>("p1", null),
        new Property<>("p2", null),
        new Property<>("p3", null),
        new Property<>("p4", null)
    );
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "At most one of [Property{name='p1', value=null}, Property{name='p2', value=null}, "
        + "Property{name='p3', value=null}, Property{name='p4', value=null}] must be present"
    );
    Checks.checkOneNotNullOrEmpty(properties);
  }
}
