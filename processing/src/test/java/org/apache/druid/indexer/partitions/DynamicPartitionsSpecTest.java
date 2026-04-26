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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class DynamicPartitionsSpecTest
{
  @Test
  public void testConstructorWithValidParameters()
  {
    DynamicPartitionsSpec spec = new DynamicPartitionsSpec(1000, 10000L);
    Assertions.assertEquals(1000, spec.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(10000L, spec.getMaxTotalRows().longValue());

    spec = new DynamicPartitionsSpec(1, 1L);
    Assertions.assertEquals(1, spec.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(1L, spec.getMaxTotalRows().longValue());

    spec = new DynamicPartitionsSpec(null, 5000L);
    Assertions.assertEquals(5000L, spec.getMaxTotalRows().longValue());
    Assertions.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());

    spec = new DynamicPartitionsSpec(500, null);
    Assertions.assertEquals(500, spec.getMaxRowsPerSegment().intValue());
    Assertions.assertNull(spec.getMaxTotalRows());

    spec = new DynamicPartitionsSpec(-1, 3333L);
    Assertions.assertEquals(3333L, spec.getMaxTotalRows().longValue());
    Assertions.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, spec.getMaxRowsPerSegment().intValue());

    spec = new DynamicPartitionsSpec(1000, -1L);
    Assertions.assertEquals(1000, spec.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(-1L, spec.getMaxTotalRows().longValue());
  }

  @Test
  public void testConstructorWithInvalidParametersThrowsInvalidInput()
  {
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(0, 10000L));
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(1000, 0L));
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(0, 0L));
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(-2, 3333L));
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(1000, -2L));
    Assertions.assertThrows(DruidException.class, () -> new DynamicPartitionsSpec(-100, -1000L));
  }
}
