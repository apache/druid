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

package io.druid.indexer.updater;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.JobHelper;
import io.druid.indexer.Jobby;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.metadata.MetadataSegmentManagerConfig;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.metadata.TestDerbyConnector;
import io.druid.metadata.storage.derby.DerbyConnector;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.segment.TestIndex;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class HadoopConverterJobTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private String storageLocProperty = null;
  private File tmpSegmentDir = null;

  private static final String DATASOURCE = "testDatasource";
  private static final String STORAGE_PROPERTY_KEY = "druid.storage.storageDirectory";

  private Supplier<MetadataStorageTablesConfig> metadataStorageTablesConfigSupplier;
  private DerbyConnector connector;

  private final Interval interval = Intervals.of("2011-01-01T00:00:00.000Z/2011-05-01T00:00:00.000Z");

  @After
  public void tearDown()
  {
    if (storageLocProperty == null) {
      System.clearProperty(STORAGE_PROPERTY_KEY);
    } else {
      System.setProperty(STORAGE_PROPERTY_KEY, storageLocProperty);
    }
    tmpSegmentDir = null;
  }

  @Before
  public void setUp() throws Exception
  {
    final MetadataStorageUpdaterJobSpec metadataStorageUpdaterJobSpec = new MetadataStorageUpdaterJobSpec()
    {
      @Override
      public String getSegmentTable()
      {
        return derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
      }

      @Override
      public MetadataStorageConnectorConfig get()
      {
        return derbyConnectorRule.getMetadataConnectorConfig();
      }
    };
    final File scratchFileDir = temporaryFolder.newFolder();
    storageLocProperty = System.getProperty(STORAGE_PROPERTY_KEY);
    tmpSegmentDir = temporaryFolder.newFolder();
    System.setProperty(STORAGE_PROPERTY_KEY, tmpSegmentDir.getAbsolutePath());

    final URL url = Preconditions.checkNotNull(Query.class.getClassLoader().getResource("druid.sample.tsv"));
    final File tmpInputFile = temporaryFolder.newFile();
    FileUtils.retryCopy(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return url.openStream();
          }
        },
        tmpInputFile,
        FileUtils.IS_EXCEPTION,
        3
    );
    final HadoopDruidIndexerConfig hadoopDruidIndexerConfig = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                DATASOURCE,
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new DelimitedParseSpec(
                            new TimestampSpec("ts", "iso", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList(TestIndex.DIMENSIONS)), null, null),
                            "\t",
                            "\u0001",
                            Arrays.asList(TestIndex.COLUMNS),
                            false,
                            0
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new DoubleSumAggregatorFactory(TestIndex.DOUBLE_METRICS[0], TestIndex.DOUBLE_METRICS[0]),
                    new HyperUniquesAggregatorFactory("quality_uniques", "quality")
                },
                new UniformGranularitySpec(
                    Granularities.MONTH,
                    Granularities.DAY,
                    ImmutableList.<Interval>of(interval)
                ),
                null,
                HadoopDruidIndexerConfig.JSON_MAPPER
            ),
            new HadoopIOConfig(
                ImmutableMap.<String, Object>of(
                    "type", "static",
                    "paths", tmpInputFile.getAbsolutePath()
                ),
                metadataStorageUpdaterJobSpec,
                tmpSegmentDir.getAbsolutePath()
            ),
            new HadoopTuningConfig(
                scratchFileDir.getAbsolutePath(),
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                false,
                null,
                false,
                false,
                null,
                null,
                null,
                false,
                false,
                null,
                null,
                null
            )
        )
    );
    metadataStorageTablesConfigSupplier = derbyConnectorRule.metadataTablesConfigSupplier();
    connector = derbyConnectorRule.getConnector();

    try {
      connector.getDBI().withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              handle.execute("DROP TABLE druid_segments");
              return null;
            }
          }
      );
    }
    catch (CallbackFailedException e) {
      // Who cares
    }
    List<Jobby> jobs = ImmutableList.of(
        new Jobby()
        {
          @Override
          public boolean run()
          {
            connector.createSegmentTable(metadataStorageUpdaterJobSpec.getSegmentTable());
            return true;
          }
        },
        new HadoopDruidDetermineConfigurationJob(hadoopDruidIndexerConfig),
        new HadoopDruidIndexerJob(
            hadoopDruidIndexerConfig,
            new SQLMetadataStorageUpdaterJobHandler(connector)
        )
    );
    Assert.assertTrue(JobHelper.runJobs(jobs, hadoopDruidIndexerConfig));
  }

  private List<DataSegment> getDataSegments(
      SQLMetadataSegmentManager manager
  ) throws InterruptedException
  {
    manager.start();
    while (!manager.isStarted()) {
      Thread.sleep(10);
    }
    manager.poll();
    final ImmutableDruidDataSource druidDataSource = manager.getInventoryValue(DATASOURCE);
    manager.stop();
    return Lists.newArrayList(druidDataSource.getSegments());
  }

  @Test
  public void testSimpleJob() throws IOException, InterruptedException
  {

    final SQLMetadataSegmentManager manager = new SQLMetadataSegmentManager(
        HadoopDruidConverterConfig.jsonMapper,
        new Supplier<MetadataSegmentManagerConfig>()
        {
          @Override
          public MetadataSegmentManagerConfig get()
          {
            return new MetadataSegmentManagerConfig();
          }
        },
        metadataStorageTablesConfigSupplier,
        connector
    );

    final List<DataSegment> oldSemgments = getDataSegments(manager);
    final File tmpDir = temporaryFolder.newFolder();
    final HadoopConverterJob converterJob = new HadoopConverterJob(
        new HadoopDruidConverterConfig(
            DATASOURCE,
            interval,
            new IndexSpec(new RoaringBitmapSerdeFactory(null),
                          CompressionStrategy.UNCOMPRESSED,
                          CompressionStrategy.UNCOMPRESSED,
                          CompressionFactory.LongEncodingStrategy.LONGS),
            oldSemgments,
            true,
            tmpDir.toURI(),
            ImmutableMap.<String, String>of(),
            null,
            tmpSegmentDir.toURI().toString()
        )
    );

    final List<DataSegment> segments = Lists.newArrayList(converterJob.run());
    Assert.assertNotNull("bad result", segments);
    Assert.assertEquals("wrong segment count", 4, segments.size());
    Assert.assertTrue(converterJob.getLoadedBytes() > 0);
    Assert.assertTrue(converterJob.getWrittenBytes() > 0);
    Assert.assertTrue(converterJob.getWrittenBytes() > converterJob.getLoadedBytes());

    Assert.assertEquals(oldSemgments.size(), segments.size());

    final DataSegment segment = segments.get(0);
    Assert.assertTrue(interval.contains(segment.getInterval()));
    Assert.assertTrue(segment.getVersion().endsWith("_converted"));
    Assert.assertTrue(segment.getLoadSpec().get("path").toString().contains("_converted"));

    for (File file : tmpDir.listFiles()) {
      Assert.assertFalse(file.isDirectory());
      Assert.assertTrue(file.isFile());
    }


    final Comparator<DataSegment> segmentComparator = new Comparator<DataSegment>()
    {
      @Override
      public int compare(DataSegment o1, DataSegment o2)
      {
        return o1.getIdentifier().compareTo(o2.getIdentifier());
      }
    };
    Collections.sort(
        oldSemgments,
        segmentComparator
    );
    Collections.sort(
        segments,
        segmentComparator
    );

    for (int i = 0; i < oldSemgments.size(); ++i) {
      final DataSegment oldSegment = oldSemgments.get(i);
      final DataSegment newSegment = segments.get(i);
      Assert.assertEquals(oldSegment.getDataSource(), newSegment.getDataSource());
      Assert.assertEquals(oldSegment.getInterval(), newSegment.getInterval());
      Assert.assertEquals(
          Sets.<String>newHashSet(oldSegment.getMetrics()),
          Sets.<String>newHashSet(newSegment.getMetrics())
      );
      Assert.assertEquals(
          Sets.<String>newHashSet(oldSegment.getDimensions()),
          Sets.<String>newHashSet(newSegment.getDimensions())
      );
      Assert.assertEquals(oldSegment.getVersion() + "_converted", newSegment.getVersion());
      Assert.assertTrue(oldSegment.getSize() < newSegment.getSize());
      Assert.assertEquals(oldSegment.getBinaryVersion(), newSegment.getBinaryVersion());
    }
  }

  private static void corrupt(
      DataSegment segment
  ) throws IOException
  {
    final Map<String, Object> localLoadSpec = segment.getLoadSpec();
    final Path segmentPath = Paths.get(localLoadSpec.get("path").toString());

    final MappedByteBuffer buffer = Files.map(segmentPath.toFile(), FileChannel.MapMode.READ_WRITE);
    while (buffer.hasRemaining()) {
      buffer.put((byte) 0xFF);
    }
  }

  @Test
  @Ignore // This takes a long time due to retries
  public void testHadoopFailure() throws IOException, InterruptedException
  {
    final SQLMetadataSegmentManager manager = new SQLMetadataSegmentManager(
        HadoopDruidConverterConfig.jsonMapper,
        new Supplier<MetadataSegmentManagerConfig>()
        {
          @Override
          public MetadataSegmentManagerConfig get()
          {
            return new MetadataSegmentManagerConfig();
          }
        },
        metadataStorageTablesConfigSupplier,
        connector
    );

    final List<DataSegment> oldSemgments = getDataSegments(manager);
    final File tmpDir = temporaryFolder.newFolder();
    final HadoopConverterJob converterJob = new HadoopConverterJob(
        new HadoopDruidConverterConfig(
            DATASOURCE,
            interval,
            new IndexSpec(new RoaringBitmapSerdeFactory(null),
                          CompressionStrategy.UNCOMPRESSED,
                          CompressionStrategy.UNCOMPRESSED,
                          CompressionFactory.LongEncodingStrategy.LONGS),
            oldSemgments,
            true,
            tmpDir.toURI(),
            ImmutableMap.<String, String>of(),
            null,
            tmpSegmentDir.toURI().toString()
        )
    );

    corrupt(oldSemgments.get(0));

    final List<DataSegment> result = converterJob.run();
    Assert.assertNull("result should be null", result);

    final List<DataSegment> segments = getDataSegments(manager);

    Assert.assertEquals(oldSemgments.size(), segments.size());

    Assert.assertEquals(oldSemgments, segments);
  }
}
