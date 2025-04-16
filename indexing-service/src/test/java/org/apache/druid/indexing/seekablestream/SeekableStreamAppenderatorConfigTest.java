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

package org.apache.druid.indexing.seekablestream;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.query.DruidProcessingBufferConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.AppendableIndexBuilder;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.utils.RuntimeInfo;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

public class SeekableStreamAppenderatorConfigTest
{
  @Test
  public void test_fromTuningConfig()
  {
    final SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    Mockito.when(tuningConfig.isReportParseExceptions()).thenReturn(true);
    Mockito.when(tuningConfig.getMaxPendingPersists()).thenReturn(2);
    Mockito.when(tuningConfig.isSkipBytesInMemoryOverheadCheck()).thenReturn(true);
    Mockito.when(tuningConfig.getIntermediatePersistPeriod()).thenReturn(Period.days(3));
    Mockito.when(tuningConfig.getBasePersistDirectory()).thenReturn(new File("/nonexistent/tmp"));
    Mockito.when(tuningConfig.getMaxRowsInMemory()).thenReturn(11);
    Mockito.when(tuningConfig.getMaxBytesInMemory()).thenReturn(12L);
    Mockito.when(tuningConfig.getIndexSpec())
           .thenReturn(IndexSpec.builder().withMetricCompression(CompressionStrategy.NONE).build());
    Mockito.when(tuningConfig.getIndexSpecForIntermediatePersists())
           .thenReturn(IndexSpec.builder().withMetricCompression(CompressionStrategy.NONE).build());
    Mockito.when(tuningConfig.getNumPersistThreads()).thenReturn(13);
    Mockito.when(tuningConfig.getMaxRowsPerSegment()).thenReturn(14);
    Mockito.when(tuningConfig.getMaxTotalRows()).thenReturn(15L);
    Mockito.when(tuningConfig.getMaxColumnsToMerge()).thenReturn(16);
    Mockito.when(tuningConfig.isSkipBytesInMemoryOverheadCheck()).thenReturn(true);
    Mockito.when(tuningConfig.getMaxColumnsToMerge()).thenReturn(123);

    final SeekableStreamAppenderatorConfig appenderatorConfig =
        SeekableStreamAppenderatorConfig.fromTuningConfig(tuningConfig, null);

    Assert.assertEquals(tuningConfig.isReportParseExceptions(), appenderatorConfig.isReportParseExceptions());
    Assert.assertEquals(tuningConfig.getMaxPendingPersists(), appenderatorConfig.getMaxPendingPersists());
    Assert.assertEquals(
        tuningConfig.isSkipBytesInMemoryOverheadCheck(),
        appenderatorConfig.isSkipBytesInMemoryOverheadCheck()
    );
    Assert.assertEquals(tuningConfig.getIntermediatePersistPeriod(), appenderatorConfig.getIntermediatePersistPeriod());
    Assert.assertEquals(tuningConfig.getBasePersistDirectory(), appenderatorConfig.getBasePersistDirectory());
    Assert.assertEquals(tuningConfig.getMaxRowsInMemory(), appenderatorConfig.getMaxRowsInMemory());
    Assert.assertEquals(tuningConfig.getMaxBytesInMemory(), appenderatorConfig.getMaxBytesInMemory());
    Assert.assertEquals(tuningConfig.getIndexSpec(), appenderatorConfig.getIndexSpec());
    Assert.assertEquals(
        tuningConfig.getIndexSpecForIntermediatePersists(),
        appenderatorConfig.getIndexSpecForIntermediatePersists()
    );
    Assert.assertEquals(tuningConfig.getNumPersistThreads(), appenderatorConfig.getNumPersistThreads());
    Assert.assertEquals(tuningConfig.getMaxRowsPerSegment(), appenderatorConfig.getMaxRowsPerSegment());
    Assert.assertEquals(tuningConfig.getMaxTotalRows(), appenderatorConfig.getMaxTotalRows());
    Assert.assertEquals(tuningConfig.getMaxColumnsToMerge(), appenderatorConfig.getMaxColumnsToMerge());
    Assert.assertEquals(
        tuningConfig.isSkipBytesInMemoryOverheadCheck(),
        appenderatorConfig.isSkipBytesInMemoryOverheadCheck()
    );
    Assert.assertEquals(tuningConfig.getMaxColumnsToMerge(), appenderatorConfig.getMaxColumnsToMerge());
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_direct2g_xmx1g_maxBytesAuto_2proc_1merge()
  {
    Assert.assertEquals(
        17293,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new DruidProcessingConfigTest.MockRuntimeInfo(2, 2_000_000_000L, 1_000_000_000L),
            new MockProcessingConfig(2, 1, 100_000_000),
            new MockTuningConfig(0, 500_000_000L)
        )
    );
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_direct2g_xmx1g_maxBytesUnlimited_2proc_1merge()
  {
    Assert.assertEquals(
        17293,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new DruidProcessingConfigTest.MockRuntimeInfo(2, 2_000_000_000L, 1_000_000_000L),
            new MockProcessingConfig(2, 1, 100_000_000),
            new MockTuningConfig(-1, 500_000_000L)
        )
    );
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_direct1800m_xmx1g_maxBytesUnlimited_2proc_1merge()
  {
    Assert.assertEquals(
        15258,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new DruidProcessingConfigTest.MockRuntimeInfo(2, 1_800_000_000L, 1_000_000_000L),
            new MockProcessingConfig(2, 1, 100_000_000),
            new MockTuningConfig(-1, 500_000_000L)
        )
    );
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_direct1800m_xmx1g_maxBytesUnlimited_3proc_2merge()
  {
    Assert.assertEquals(
        13224,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new DruidProcessingConfigTest.MockRuntimeInfo(3, 1_800_000_000L, 1_000_000_000L),
            new MockProcessingConfig(3, 2, 100_000_000),
            new MockTuningConfig(-1, 500_000_000L)
        )
    );
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_direct2g_xmx1g_maxBytes20m_2proc_1merge()
  {
    Assert.assertEquals(
        6666,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new DruidProcessingConfigTest.MockRuntimeInfo(2, 2_000_000_000L, 1_000_000_000L),
            new MockProcessingConfig(2, 1, 100_000_000),
            new MockTuningConfig(20_000_000L, 500_000_000L)
        )
    );
  }

  @Test
  public void test_calculateDefaultMaxColumnsToMerge_directUnsupported_xmx1g_maxBytes20m_2proc_1merge()
  {
    Assert.assertEquals(
        1017,
        SeekableStreamAppenderatorConfig.calculateDefaultMaxColumnsToMerge(
            new RuntimeInfo()
            {
              @Override
              public long getDirectMemorySizeBytes()
              {
                throw new UnsupportedOperationException();
              }
            },
            new MockProcessingConfig(2, 1, 100_000_000),
            new MockTuningConfig(20_000_000L, 500_000_000L)
        )
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(SeekableStreamAppenderatorConfig.class)
                  .usingGetClass()
                  .verify();
  }

  private static class MockProcessingConfig extends DruidProcessingConfig
  {
    public MockProcessingConfig(final int numThreads, final int numMergeBuffers, final int bufferSize)
    {
      super(
          null,
          numThreads,
          numMergeBuffers,
          null,
          null,
          new DruidProcessingBufferConfig(HumanReadableBytes.valueOf(bufferSize), null, null),
          null
      );
    }
  }

  private static class MockTuningConfig implements TuningConfig
  {
    private final long configuredMaxBytesInMemory;
    private final long defaultMaxBytesInMemory;

    public MockTuningConfig(long configuredMaxBytesInMemory, long defaultMaxBytesInMemory)
    {
      this.configuredMaxBytesInMemory = configuredMaxBytesInMemory;
      this.defaultMaxBytesInMemory = defaultMaxBytesInMemory;
    }

    @Override
    public AppendableIndexSpec getAppendableIndexSpec()
    {
      return new AppendableIndexSpec()
      {
        @Override
        public AppendableIndexBuilder builder()
        {
          throw new UnsupportedOperationException();
        }

        @Override
        public long getDefaultMaxBytesInMemory()
        {
          return defaultMaxBytesInMemory;
        }
      };
    }

    @Override
    public int getMaxRowsInMemory()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxBytesInMemory()
    {
      return configuredMaxBytesInMemory;
    }

    @Override
    public PartitionsSpec getPartitionsSpec()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexSpec getIndexSpec()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexSpec getIndexSpecForIntermediatePersists()
    {
      throw new UnsupportedOperationException();
    }
  }
}
