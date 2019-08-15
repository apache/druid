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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ParallelIndexTuningConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setup()
  {
    mapper.registerSubtypes(new NamedType(ParallelIndexTuningConfig.class, "index_parallel"));
  }

  @Test
  public void testSerdeDefault() throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = ParallelIndexTuningConfig.defaultConfig();
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testSerdeWithMaxRowsPerSegment()
      throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        10,
        1000L,
        null,
        null,
        new DynamicPartitionsSpec(100, 100L),
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.UNCOMPRESSED,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        new IndexSpec(),
        1,
        false,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        250,
        100,
        20,
        new Duration(3600),
        128,
        false,
        null,
        null
    );
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }
}
