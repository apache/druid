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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class NestedDataColumnIndexerTest extends InitializedNullHandlingTest
{
  @Test
  public void testStuff()
  {
    NestedDataColumnIndexer indexer = new NestedDataColumnIndexer();
    Assert.assertEquals(0, indexer.getCardinality());

    EncodedKeyComponent<StructuredData> key;
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(230, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(112, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(94, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());
    // new raw value, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(11L, false);
    Assert.assertEquals(48, key.getEffectiveSizeBytes());
    Assert.assertEquals(3, indexer.getCardinality());

    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(276, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, re-use fields and dictionary
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(56, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)), false);
    Assert.assertEquals(292, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)), false);
    Assert.assertEquals(118, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());

    key = indexer.processRowValsToUnsortedEncodedKeyComponent("", false);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    } else {
      Assert.assertEquals(104, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    }
  }
}
