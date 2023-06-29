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

package org.apache.druid.frame.key;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.guava.Comparators;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ClusterByTest
{
  @Test
  public void test_keyComparator()
  {
    final ImmutableList<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("x", KeyOrder.ASCENDING),
        new KeyColumn("y", KeyOrder.ASCENDING)
    );

    Assert.assertEquals(
        RowKeyComparator.create(keyColumns),
        new ClusterBy(keyColumns, 1).keyComparator()
    );
  }

  @Test
  public void test_bucketComparator_noKey()
  {
    Assert.assertSame(Comparators.alwaysEqual(), ClusterBy.none().bucketComparator());
  }

  @Test
  public void test_bucketComparator_noBucketKey()
  {
    Assert.assertSame(
        Comparators.alwaysEqual(),
        new ClusterBy(
            ImmutableList.of(
                new KeyColumn("x", KeyOrder.ASCENDING),
                new KeyColumn("y", KeyOrder.ASCENDING)
            ),
            0
        ).bucketComparator()
    );
  }

  @Test
  public void test_bucketComparator_withBucketKey()
  {
    Assert.assertEquals(
        RowKeyComparator.create(ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING))),
        new ClusterBy(
            ImmutableList.of(
                new KeyColumn("x", KeyOrder.ASCENDING),
                new KeyColumn("y", KeyOrder.ASCENDING)
            ),
            1
        ).bucketComparator()
    );
  }

  @Test
  public void test_sortable()
  {
    Assert.assertFalse(
        new ClusterBy(
            ImmutableList.of(
                new KeyColumn("x", KeyOrder.NONE),
                new KeyColumn("y", KeyOrder.NONE)
            ),
            0
        ).sortable()
    );

    Assert.assertTrue(
        new ClusterBy(
            ImmutableList.of(
                new KeyColumn("x", KeyOrder.ASCENDING),
                new KeyColumn("y", KeyOrder.ASCENDING)
            ),
            0
        ).sortable()
    );

    Assert.assertTrue(new ClusterBy(Collections.emptyList(), 0).sortable());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ClusterBy.class)
                  .usingGetClass()
                  .withIgnoredFields("sortable")
                  .verify();
  }
}
