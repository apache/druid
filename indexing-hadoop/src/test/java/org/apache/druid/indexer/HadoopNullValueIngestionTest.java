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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.hadoop.DatasourceRecordReader;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.realtime.WindowedCursorFactory;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * End-to-end test for null value handling in Hadoop batch ingestion.
 * Tests that null values in multi-value dimensions are preserved through
 * the ingestion pipeline (serde, indexing, segment creation).
 */
public class HadoopNullValueIngestionTest
{
  private static final ObjectMapper MAPPER;
  private static final IndexIO INDEX_IO;
  private static final Interval INTERVAL = Intervals.of("2024-01-01T00:00:00Z/P1D");

  static {
    MAPPER = new DefaultObjectMapper();
    MAPPER.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    InjectableValues inject = new InjectableValues.Std()
        .addValue(ObjectMapper.class, MAPPER)
        .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(inject);
    INDEX_IO = HadoopDruidIndexerConfig.INDEX_IO;
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Tests that null values in multi-value string dimensions are preserved.
   * This tests the fix for the issue where null values were being serialized
   * as the string "null" instead of being preserved as actual null.
   */
  @Test
  public void testMultiValueDimensionWithNullValues() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File dataFile = new File(tmpDir, "data.json");

    // Create test data with null values in multi-value dimension
    // Using unique dim values to identify rows
    List<String> data = ImmutableList.of(
        "{\"ts\":\"2024-01-01T00:00:00Z\", \"dim\": \"mixed_null\", \"tags\": [\"a\", null, \"b\"]}",
        "{\"ts\":\"2024-01-01T01:00:00Z\", \"dim\": \"only_null\", \"tags\": [null]}",
        "{\"ts\":\"2024-01-01T02:00:00Z\", \"dim\": \"no_null\", \"tags\": [\"x\", \"y\"]}"
    );
    FileUtils.writeLines(dataFile, data);

    HadoopDruidIndexerConfig config = makeConfig(dataFile, tmpDir, ImmutableList.of("dim", "tags"));

    // Run ingestion
    IndexGeneratorJob job = new IndexGeneratorJob(config);
    Assert.assertTrue("Ingestion job should succeed", JobHelper.runJobs(ImmutableList.of(job)));

    List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths =
        IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(config);
    JobHelper.renameIndexFilesForSegments(config.getSchema(), dataSegmentAndIndexZipFilePaths);

    // Load and verify the segment
    File segmentFolder = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/0",
            config.getSchema().getIOConfig().getSegmentOutputPath(),
            config.getSchema().getDataSchema().getDataSource(),
            INTERVAL.getStart().toString(),
            INTERVAL.getEnd().toString(),
            config.getSchema().getTuningConfig().getVersion()
        )
    );
    Assert.assertTrue("Segment folder should exist", segmentFolder.exists());

    File indexZip = new File(segmentFolder, "index.zip");
    Assert.assertTrue("index.zip should exist", indexZip.exists());

    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(indexZip, tmpUnzippedSegmentDir);

    QueryableIndex index = INDEX_IO.loadIndex(tmpUnzippedSegmentDir);

    // Read rows from the segment
    DatasourceRecordReader.SegmentReader segmentReader = new DatasourceRecordReader.SegmentReader(
        ImmutableList.of(new WindowedCursorFactory(new QueryableIndexCursorFactory(index), INTERVAL)),
        TransformSpec.NONE,
        ImmutableList.of("dim", "tags"),
        ImmutableList.of("count"),
        null
    );

    List<InputRow> rows = new ArrayList<>();
    while (segmentReader.hasMore()) {
      rows.add(segmentReader.nextRow());
    }
    segmentReader.close();

    Assert.assertEquals("Should have 3 rows", 3, rows.size());

    // Find rows by their dim values
    InputRow mixedNullRow = null;
    InputRow onlyNullRow = null;
    InputRow noNullRow = null;

    for (InputRow row : rows) {
      List<String> dimVals = row.getDimension("dim");
      if (dimVals != null && !dimVals.isEmpty()) {
        String dimVal = dimVals.get(0);
        if ("mixed_null".equals(dimVal)) {
          mixedNullRow = row;
        } else if ("only_null".equals(dimVal)) {
          onlyNullRow = row;
        } else if ("no_null".equals(dimVal)) {
          noNullRow = row;
        }
      }
    }

    Assert.assertNotNull("Should find mixed_null row", mixedNullRow);
    Assert.assertNotNull("Should find only_null row", onlyNullRow);
    Assert.assertNotNull("Should find no_null row", noNullRow);

    // Verify mixed_null row: tags = ["a", null, "b"]
    // Note: Multi-value dimensions are sorted when stored in segments.
    // Null sorts first, so ["a", null, "b"] becomes [null, "a", "b"]
    List<String> mixedTags = mixedNullRow.getDimension("tags");
    Assert.assertEquals("mixed_null row tags should have 3 elements", 3, mixedTags.size());
    // The key assertion: null is preserved as actual null, not converted to string "null"
    Assert.assertTrue(
        "Should contain a null value (not string 'null')",
        mixedTags.contains(null)
    );
    Assert.assertFalse(
        "Should NOT contain string 'null' - that would indicate incorrect serialization",
        mixedTags.contains("null")
    );
    // Verify all expected values are present (accounting for sort order)
    Assert.assertTrue("Should contain 'a'", mixedTags.contains("a"));
    Assert.assertTrue("Should contain 'b'", mixedTags.contains("b"));
    // Verify sorted order: null sorts first
    Assert.assertNull("First element should be null (null sorts first)", mixedTags.get(0));
    Assert.assertEquals("Second element should be 'a'", "a", mixedTags.get(1));
    Assert.assertEquals("Third element should be 'b'", "b", mixedTags.get(2));

    // Verify only_null row: tags = [null]
    // When a dimension has only null values, Druid returns an empty list from getDimension()
    // This is expected Druid behavior - the key is that it doesn't return ["null"] (string)
    List<String> onlyNullTags = onlyNullRow.getDimension("tags");
    Assert.assertTrue(
        "only_null row tags should be empty (Druid returns empty list for all-null dimension)",
        onlyNullTags.isEmpty()
    );
    Assert.assertFalse(
        "Should NOT contain string 'null' - that would indicate incorrect serialization",
        onlyNullTags.contains("null")
    );

    // Verify no_null row: tags = ["x", "y"]
    List<String> noNullTags = noNullRow.getDimension("tags");
    Assert.assertEquals(Arrays.asList("x", "y"), noNullTags);
    Assert.assertFalse("Should not contain null", noNullTags.contains(null));
  }

  /**
   * Tests that a completely null dimension value is handled correctly.
   */
  @Test
  public void testNullDimensionValue() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File dataFile = new File(tmpDir, "data.json");

    // Create test data where "tags" dimension is null (not present or explicitly null)
    List<String> data = ImmutableList.of(
        "{\"ts\":\"2024-01-01T00:00:00Z\", \"dim\": \"row1\", \"tags\": null}",
        "{\"ts\":\"2024-01-01T01:00:00Z\", \"dim\": \"row2\"}",
        "{\"ts\":\"2024-01-01T02:00:00Z\", \"dim\": \"row3\", \"tags\": \"value\"}"
    );
    FileUtils.writeLines(dataFile, data);

    HadoopDruidIndexerConfig config = makeConfig(dataFile, tmpDir, ImmutableList.of("dim", "tags"));

    // Run ingestion
    IndexGeneratorJob job = new IndexGeneratorJob(config);
    Assert.assertTrue("Ingestion job should succeed", JobHelper.runJobs(ImmutableList.of(job)));

    List<DataSegmentAndIndexZipFilePath> dataSegmentAndIndexZipFilePaths =
        IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(config);
    JobHelper.renameIndexFilesForSegments(config.getSchema(), dataSegmentAndIndexZipFilePaths);

    // Load and verify the segment
    File segmentFolder = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/0",
            config.getSchema().getIOConfig().getSegmentOutputPath(),
            config.getSchema().getDataSchema().getDataSource(),
            INTERVAL.getStart().toString(),
            INTERVAL.getEnd().toString(),
            config.getSchema().getTuningConfig().getVersion()
        )
    );

    File indexZip = new File(segmentFolder, "index.zip");
    Assert.assertTrue("index.zip should exist", indexZip.exists());

    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(indexZip, tmpUnzippedSegmentDir);

    QueryableIndex index = INDEX_IO.loadIndex(tmpUnzippedSegmentDir);

    // Read rows from the segment
    DatasourceRecordReader.SegmentReader segmentReader = new DatasourceRecordReader.SegmentReader(
        ImmutableList.of(new WindowedCursorFactory(new QueryableIndexCursorFactory(index), INTERVAL)),
        TransformSpec.NONE,
        ImmutableList.of("dim", "tags"),
        ImmutableList.of("count"),
        null
    );

    List<InputRow> rows = new ArrayList<>();
    while (segmentReader.hasMore()) {
      rows.add(segmentReader.nextRow());
    }
    segmentReader.close();

    Assert.assertEquals("Should have 3 rows", 3, rows.size());

    // Sort rows by dim to ensure consistent ordering
    rows.sort(Comparator.comparing(a -> String.valueOf(a.getDimension("dim").get(0))));

    // Row 1: tags = null
    InputRow row1 = rows.get(0);
    Assert.assertEquals("row1", row1.getDimension("dim").get(0));
    List<String> tags1 = row1.getDimension("tags");
    // Null dimension returns empty list or list with null
    Assert.assertTrue(
        "Null dimension should return empty or single-null list",
        tags1.isEmpty() || (tags1.size() == 1 && tags1.get(0) == null)
    );

    // Row 2: tags not present (missing)
    InputRow row2 = rows.get(1);
    Assert.assertEquals("row2", row2.getDimension("dim").get(0));
    List<String> tags2 = row2.getDimension("tags");
    Assert.assertTrue(
        "Missing dimension should return empty or single-null list",
        tags2.isEmpty() || (tags2.size() == 1 && tags2.get(0) == null)
    );

    // Row 3: tags = "value"
    InputRow row3 = rows.get(2);
    Assert.assertEquals("row3", row3.getDimension("dim").get(0));
    Assert.assertEquals(Collections.singletonList("value"), row3.getDimension("tags"));
  }

  private HadoopDruidIndexerConfig makeConfig(File dataFile, File tmpDir, List<String> dimensions) throws Exception
  {
    File outputDir = new File(tmpDir, "output");

    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("test_null_values")
                      .withParserMap(MAPPER.convertValue(
                          new StringInputRowParser(
                              new JSONParseSpec(
                                  new TimestampSpec("ts", "iso", null),
                                  new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)),
                                  null,
                                  null,
                                  null
                              ),
                              null
                          ),
                          Map.class
                      ))
                      .withAggregators(new CountAggregatorFactory("count"))
                      .withGranularity(new UniformGranularitySpec(
                          Granularities.DAY,
                          Granularities.NONE,
                          ImmutableList.of(INTERVAL)
                      ))
                      .withObjectMapper(MAPPER)
                      .build(),
            new HadoopIOConfig(
                ImmutableMap.of(
                    "type", "static",
                    "paths", dataFile.getCanonicalPath()
                ),
                null,
                outputDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
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
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                1
            )
        )
    );

    config.setShardSpecs(
        ImmutableMap.of(
            INTERVAL.getStartMillis(),
            ImmutableList.of(
                new HadoopyShardSpec(
                    new HashBasedNumberedShardSpec(
                        0,
                        1,
                        0,
                        1,
                        null,
                        HashPartitionFunction.MURMUR3_32_ABS,
                        HadoopDruidIndexerConfig.JSON_MAPPER
                    ),
                    0
                )
            )
        )
    );
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
    return config;
  }
}
