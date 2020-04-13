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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class PrefixFilteredDimensionSpecTest
{

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();

    String jsonStr = "{\n"
                     + "  \"type\": \"prefixFiltered\",\n"
                     + "  \"delegate\": {\n"
                     + "    \"type\": \"default\",\n"
                     + "    \"dimension\": \"foo\",\n"
                     + "    \"outputName\": \"bar\"\n"
                     + "  },\n"
                     + "  \"prefix\": \"xxx\"\n"
                     + "}";

    PrefixFilteredDimensionSpec actual = (PrefixFilteredDimensionSpec) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, DimensionSpec.class)),
        DimensionSpec.class);

    PrefixFilteredDimensionSpec expected = new PrefixFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xxx"
    );

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetCacheKey()
  {
    PrefixFilteredDimensionSpec spec1 = new PrefixFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xxx"
    );

    PrefixFilteredDimensionSpec spec2 = new PrefixFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "bar"),
        "xyz"
    );

    Assert.assertFalse(Arrays.equals(spec1.getCacheKey(), spec2.getCacheKey()));
  }

  @Test
  public void testDecorator()
  {
    PrefixFilteredDimensionSpec spec = new PrefixFilteredDimensionSpec(
        new DefaultDimensionSpec("foo", "far"),
        "c"
    );

    DimensionSelector selector = spec.decorate(TestDimensionSelector.INSTANCE);

    Assert.assertEquals(1, selector.getValueCardinality());

    IndexedInts row = selector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    Assert.assertEquals("c", selector.lookupName(0));

    Assert.assertEquals(0, selector.idLookup().lookupId("c"));
  }
}
