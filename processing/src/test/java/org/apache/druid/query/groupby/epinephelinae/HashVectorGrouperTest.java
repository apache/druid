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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.com.google.common.base.Suppliers;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class HashVectorGrouperTest
{
  @Test
  public void testCloseAggregatorAdaptorsShouldBeClosed()
  {
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[4096]);
    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        1024,
        aggregatorAdapters,
        Integer.MAX_VALUE,
        0.f,
        0
    );
    grouper.initVectorized(512);
    grouper.close();
    Mockito.verify(aggregatorAdapters, Mockito.times(1)).close();
  }
}
