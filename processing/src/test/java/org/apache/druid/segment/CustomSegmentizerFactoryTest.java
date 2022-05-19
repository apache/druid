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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class CustomSegmentizerFactoryTest extends InitializedNullHandlingTest
{
  private static ObjectMapper JSON_MAPPER;
  private static IndexIO INDEX_IO;
  private static IndexMerger INDEX_MERGER;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(new SegmentizerModule());
    mapper.registerSubtypes(new NamedType(CustomSegmentizerFactory.class, "customSegmentFactory"));
    final IndexIO indexIO = new IndexIO(mapper, () -> 0);

    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(IndexIO.class, indexIO)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
    );

    JSON_MAPPER = mapper;
    INDEX_IO = indexIO;
    INDEX_MERGER = new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Test
  public void testDefaultSegmentizerPersist() throws IOException
  {
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");
    File segment = new File(temporaryFolder.newFolder(), "segment");
    File persisted = INDEX_MERGER.persist(
        data,
        Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z"),
        segment,
        new IndexSpec(
            null,
            null,
            null,
            null,
            null
        ),
        null
    );

    File factoryJson = new File(persisted, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    SegmentizerFactory factory = JSON_MAPPER.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof MMappedQueryableSegmentizerFactory);
  }

  @Test
  public void testCustomSegmentizerPersist() throws IOException
  {
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");
    File segment = new File(temporaryFolder.newFolder(), "segment");
    File persisted = INDEX_MERGER.persist(
        data,
        Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z"),
        segment,
        new IndexSpec(
            null,
            null,
            null,
            null,
            new CustomSegmentizerFactory()
        ),
        null
    );

    File factoryJson = new File(persisted, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    SegmentizerFactory factory = JSON_MAPPER.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof CustomSegmentizerFactory);
  }

  private static class CustomSegmentizerFactory implements SegmentizerFactory
  {
    @Override
    public Segment factorize(DataSegment segment, File parentDir, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException
    {
      try {
        return new QueryableIndexSegment(INDEX_IO.loadIndex(parentDir, lazy, loadFailed), segment.getId());
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "%s", e.getMessage());
      }
    }
  }
}
