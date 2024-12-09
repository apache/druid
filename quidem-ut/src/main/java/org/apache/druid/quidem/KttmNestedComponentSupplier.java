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

package org.apache.druid.quidem;

import com.google.inject.Injector;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class KttmNestedComponentSupplier extends StandardComponentSupplier
{
  public KttmNestedComponentSupplier(TempDirProducer tempDirProducer)
  {
    super(tempDirProducer);
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate,
      JoinableFactoryWrapper joinableFactory, Injector injector)
  {
    SpecificSegmentsQuerySegmentWalker walker = super.createQuerySegmentWalker(conglomerate, joinableFactory, injector);
    QueryableIndex idx = makeKttmIndex(tempDirProducer.newTempFolder());

    walker.add(
        DataSegment.builder()
            .dataSource("kttm_nested")
            .interval(Intervals.of("2019-08-25/2019-08-26"))
            .version("1")
            .shardSpec(new NumberedShardSpec(0, 0))
            .size(0)
            .build(),
        idx
    );
    return walker;
  }

  public QueryableIndex makeKttmIndex(File tmpDir)
  {
    try {
      final File directory = new File(tmpDir, StringUtils.format("kttm-index-%s", UUID.randomUUID()));
      final IncrementalIndex index = makeKttmNestedIndex();
      TestIndex.INDEX_MERGER.persist(index, directory, IndexSpec.DEFAULT, null);
      return TestIndex.INDEX_IO.loadIndex(directory);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public IncrementalIndex makeKttmNestedIndex()
  {
    final List<DimensionSchema> dimensions = Arrays.asList(
        new StringDimensionSchema("session"),
        new StringDimensionSchema("number"),
        new AutoTypeColumnSchema("event", null),
        new AutoTypeColumnSchema("agent", null),
        new StringDimensionSchema("client_ip"),
        new StringDimensionSchema("geo_ip"),
        new StringDimensionSchema("language"),
        new StringDimensionSchema("adblock_list"),
        new StringDimensionSchema("app_version"),
        new StringDimensionSchema("path"),
        new StringDimensionSchema("loaded_image"),
        new StringDimensionSchema("referrer"),
        new StringDimensionSchema("referrer_host"),
        new StringDimensionSchema("server_ip"),
        new StringDimensionSchema("screen"),
        new StringDimensionSchema("window"),
        new LongDimensionSchema("session_length"),
        new StringDimensionSchema("timezone"),
        new LongDimensionSchema("timezone_offset")
    );

    final File tmpDir;
    try {
      tmpDir = FileUtils.createTempDir("test-index-input-source");
      try {
        InputSource inputSource = ResourceInputSource.of(
            TestIndex.class.getClassLoader(),
            "kttm-nested-v2-2019-08-25.json"
        );
        return IndexBuilder
            .create()
            .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
            .schema(
                new IncrementalIndexSchema.Builder()
                    .withRollup(false)
                    .withTimestampSpec(new TimestampSpec("timestamp", null, null))
                    .withDimensionsSpec(new DimensionsSpec(dimensions))
                    .build()
            )
            .inputSource(
                inputSource
            )
            .inputFormat(TestIndex.DEFAULT_JSON_INPUT_FORMAT)
            .inputTmpDir(new File(tmpDir, "tmpKttm"))
            .buildIncrementalIndex();
      }
      finally {
        FileUtils.deleteDirectory(tmpDir);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
