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

package org.apache.druid.indexing.overlord.autoscaling.gce;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class GceUtilsTest
{
  @Test
  public void testExtractNameFromInstance()
  {
    String instance0 =
        "https://www.googleapis.com/compute/v1/projects/X/zones/Y/instances/name-of-the-thing";
    Assert.assertEquals("name-of-the-thing", GceUtils.extractNameFromInstance(instance0));

    String instance1 = "https://www.googleapis.com/compute/v1/projects/X/zones/Y/instances/";
    Assert.assertEquals("", GceUtils.extractNameFromInstance(instance1));

    String instance2 = "name-of-the-thing";
    Assert.assertEquals("name-of-the-thing", GceUtils.extractNameFromInstance(instance2));

    String instance3 = null;
    Assert.assertEquals(null, GceUtils.extractNameFromInstance(instance3));

    String instance4 = "";
    Assert.assertEquals("", GceUtils.extractNameFromInstance(instance4));
  }

  @Test
  public void testBuildFilter()
  {
    List<String> list0 = null;
    try {
      String x = GceUtils.buildFilter(list0, "name");
      Assert.fail("Exception should have been thrown!");
    }
    catch (IllegalArgumentException e) {
      // ok to be here!
    }

    List<String> list1 = new ArrayList<>();
    try {
      String x = GceUtils.buildFilter(list1, "name");
      Assert.fail("Exception should have been thrown!");
    }
    catch (IllegalArgumentException e) {
      // ok to be here!
    }

    List<String> list2 = new ArrayList<>();
    list2.add("foo");
    try {
      String x = GceUtils.buildFilter(list2, null);
      Assert.fail("Exception should have been thrown!");
    }
    catch (IllegalArgumentException e) {
      // ok to be here!
    }

    List<String> list3 = new ArrayList<>();
    list3.add("foo");
    Assert.assertEquals("(name = \"foo\")", GceUtils.buildFilter(list3, "name"));

    List<String> list4 = new ArrayList<>();
    list4.add("foo");
    list4.add("bar");
    Assert.assertEquals(
        "(name = \"foo\") OR (name = \"bar\")",
        GceUtils.buildFilter(list4, "name")
    );
  }

}
