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

package org.apache.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FlattenerJsonProviderTest
{
  FlattenerJsonProvider jsonProvider = new FlattenerJsonProvider()
  {
    @Override
    public boolean isArray(final Object o)
    {
      throw new RuntimeException("not tested");
    }

    @Override
    public boolean isMap(final Object o)
    {
      throw new RuntimeException("not tested");
    }

    @Override
    public Collection<String> getPropertyKeys(final Object o)
    {
      throw new RuntimeException("not tested");
    }

    @Override
    public Object getMapValue(final Object o, final String s)
    {
      throw new RuntimeException("not tested");
    }
  };

  @Test
  public void testMapStuff()
  {
    Object aMap = jsonProvider.createMap();
    jsonProvider.setProperty(aMap, "key", "value");
    Assert.assertEquals(ImmutableMap.of("key", "value"), aMap);
    jsonProvider.removeProperty(aMap, "key");
    Assert.assertEquals(ImmutableMap.of(), aMap);
    Assert.assertEquals(aMap, jsonProvider.unwrap(aMap));

    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.setProperty(jsonProvider.createArray(), "key", "value")
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.removeProperty(jsonProvider.createArray(), "key")
    );
  }

  @Test
  public void testArrayStuff()
  {
    Object aList = jsonProvider.createArray();
    jsonProvider.setArrayIndex(aList, 0, "a");
    jsonProvider.setArrayIndex(aList, 1, "b");
    jsonProvider.setArrayIndex(aList, 2, "c");
    Assert.assertEquals(3, jsonProvider.length(aList));
    Assert.assertEquals("a", jsonProvider.getArrayIndex(aList, 0));
    Assert.assertEquals("b", jsonProvider.getArrayIndex(aList, 1));
    Assert.assertEquals("c", jsonProvider.getArrayIndex(aList, 2));
    List<String> expected = ImmutableList.of("a", "b", "c");
    Assert.assertEquals(expected, aList);
    Iterator<?> iter = jsonProvider.toIterable(aList).iterator();
    Iterator<String> expectedIter = expected.iterator();
    while (iter.hasNext()) {
      Assert.assertEquals(expectedIter.next(), iter.next());
    }
    Assert.assertFalse(expectedIter.hasNext());
    Assert.assertEquals(aList, jsonProvider.unwrap(aList));

    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.getArrayIndex(jsonProvider.createMap(), 0)
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.setArrayIndex(jsonProvider.createMap(), 0, "a")
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.toIterable(jsonProvider.createMap())
    );
  }

  @Test
  public void testNotImplementedOnPurpose()
  {
    Object aList = jsonProvider.createArray();
    Throwable t = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.toJson(aList)
    );
    Assert.assertEquals("Unused", t.getMessage());

    t = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.parse("{}")
    );
    Assert.assertEquals("Unused", t.getMessage());


    t = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.parse(new ByteArrayInputStream(StringUtils.toUtf8("{}")), "UTF-8")
    );
    Assert.assertEquals("Unused", t.getMessage());

    t = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> jsonProvider.getArrayIndex(aList, 0, false)
    );
    Assert.assertEquals("Deprecated", t.getMessage());
  }
}
