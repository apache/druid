/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class NullDimensionSelectorTest
{

  private final NullDimensionSelector selector = NullDimensionSelector.instance();

  @Test
  public void testGetRow() throws Exception
  {
    IndexedInts row = selector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(0, row.get(0));

    Iterator<Integer> iter = row.iterator();
    Assert.assertEquals(true, iter.hasNext());
    Assert.assertEquals(0, iter.next().intValue());
    Assert.assertEquals(false, iter.hasNext());
  }

  @Test
  public void testGetValueCardinality() throws Exception
  {
    Assert.assertEquals(1, selector.getValueCardinality());
  }

  @Test
  public void testLookupName() throws Exception
  {
    Assert.assertEquals(null, selector.lookupName(0));
  }

  @Test
  public void testLookupId() throws Exception
  {
    Assert.assertEquals(0, selector.idLookup().lookupId(null));
    Assert.assertEquals(0, selector.idLookup().lookupId(""));
    Assert.assertEquals(-1, selector.idLookup().lookupId("billy"));
  }
}
