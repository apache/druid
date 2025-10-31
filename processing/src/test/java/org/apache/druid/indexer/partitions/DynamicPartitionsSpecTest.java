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

package org.apache.druid.indexer.partitions;

import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;


public class DynamicPartitionsSpecTest
{
  @Test
  public void testConstructorWithValidParameters()
  {
    DynamicPartitionsSpec spec = new DynamicPartitionsSpec(1000, 10000L);
    Assert.assertEquals(1000, spec.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(10000L, spec.getMaxTotalRows().longValue());

    spec = new DynamicPartitionsSpec(1, 1L);
    Assert.assertEquals(1, spec.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(1L, spec.getMaxTotalRows().longValue());

    spec = new DynamicPartitionsSpec(null, 5000L);
    Assert.assertEquals(5000L, spec.getMaxTotalRows().longValue());
    Assert.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());

    spec = new DynamicPartitionsSpec(500, null);
    Assert.assertEquals(500, spec.getMaxRowsPerSegment().intValue());
    Assert.assertNull(spec.getMaxTotalRows());

    spec = new DynamicPartitionsSpec(-1, 3333L);
    Assert.assertEquals(3333L, spec.getMaxTotalRows().longValue());
    Assert.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());

    spec = new DynamicPartitionsSpec(1000, -1L);
    Assert.assertEquals(1000, spec.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(-1L, spec.getMaxTotalRows().longValue());
  }

  @Test
  public void testConstructorWithInvalidParametersThrowsInvalidInput()
  {
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(0, 10000L));
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(1000, 0L));
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(0, 0L));
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(-2, 3333L));
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(1000, -2L));
    Assert.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(-100, -1000L));
  }
}
