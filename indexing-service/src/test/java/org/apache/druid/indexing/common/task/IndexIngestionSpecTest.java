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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NoopFirehoseFactory;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.NoopInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IndexIngestionSpecTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParserAndInputFormat()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Cannot use parser and inputSource together. Try using inputFormat instead of parser."
    );
    final IndexIngestionSpec spec = new IndexIngestionSpec(
        new DataSchema(
            "dataSource",
            ImmutableMap.of("fake", "parser map"),
            new AggregatorFactory[0],
            new ArbitraryGranularitySpec(Granularities.NONE, null),
            null,
            null
        ),
        new IndexIOConfig(
            null,
            new NoopInputSource(),
            new NoopInputFormat(),
            null
        ),
        null
    );
  }

  @Test
  public void testParserAndInputSource()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot use parser and inputSource together.");
    final IndexIngestionSpec spec = new IndexIngestionSpec(
        new DataSchema(
            "dataSource",
            ImmutableMap.of("fake", "parser map"),
            new AggregatorFactory[0],
            new ArbitraryGranularitySpec(Granularities.NONE, null),
            null,
            null
        ),
        new IndexIOConfig(
            null,
            new NoopInputSource(),
            null,
            null
        ),
        null
    );
  }

  @Test
  public void testFirehoseAndInputSource()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "At most one of [Property{name='firehose', value=NoopFirehoseFactory{}}, Property{name='inputSource'"
    );
    final IndexIngestionSpec spec = new IndexIngestionSpec(
        new DataSchema(
            "dataSource",
            new TimestampSpec(null, null, null),
            DimensionsSpec.EMPTY,
            new AggregatorFactory[0],
            new ArbitraryGranularitySpec(Granularities.NONE, null),
            null
        ),
        new IndexIOConfig(
            new NoopFirehoseFactory(),
            new NoopInputSource(),
            null,
            null
        ),
        null
    );
  }

  @Test
  public void testFirehoseAndInputFormat()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot use firehose and inputFormat together.");
    final IndexIngestionSpec spec = new IndexIngestionSpec(
        new DataSchema(
            "dataSource",
            new TimestampSpec(null, null, null),
            DimensionsSpec.EMPTY,
            new AggregatorFactory[0],
            new ArbitraryGranularitySpec(Granularities.NONE, null),
            null
        ),
        new IndexIOConfig(
            new NoopFirehoseFactory(),
            null,
            new NoopInputFormat(),
            null
        ),
        null
    );
  }
}
