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

package io.druid.segment.incremental;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.segment.Metadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TruncateTimestampColumnRowTransformerTest
{
  @Test
  public void testTruncate()
  {
    TimestampSpec timestampSpec = new TimestampSpec("ts", "auto", null);
    QueryGranularity granularity = QueryGranularities.HOUR;
    Metadata metadata = new Metadata()
        .setTimestampSpec(timestampSpec);
    TruncateTimestampColumnRowTransformer transformer = new TruncateTimestampColumnRowTransformer(
        granularity, metadata
    );
    long timestamp = System.currentTimeMillis();
    InputRow row = transformer.apply(new MapBasedInputRow(
        timestamp,
        ImmutableList.of("ts", "dimA"),
        ImmutableMap.<String, Object>of(
            "ts", timestamp, "dimA", "dima"
        )
    ));
    assertEquals("" + granularity.truncate(timestamp), row.getDimension("ts").get(0));
    assertEquals("millis", metadata.getTimestampSpec().getTimestampFormat());
  }
}
