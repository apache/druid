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

package org.apache.druid.segment.generator;

import com.google.common.hash.Hashing;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.BaseProgressIndicator;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.Transformer;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SegmentGenerator implements Closeable
{
  private static final Logger log = new Logger(SegmentGenerator.class);

  private static final int MAX_ROWS_IN_MEMORY = 200000;

  // Setup can take a long time due to the need to generate large segments.
  // Allow users to specify a cache directory via a JVM property or an environment variable.
  private static final String CACHE_DIR_PROPERTY = "druid.benchmark.cacheDir";
  private static final String CACHE_DIR_ENV_VAR = "DRUID_BENCHMARK_CACHE_DIR";

  static {
    NullHandling.initializeForTests();
  }

  private final File cacheDir;
  private final boolean cleanupCacheDir;

  public SegmentGenerator()
  {
    this(null);
  }

  public SegmentGenerator(@Nullable final File cacheDir)
  {
    if (cacheDir != null) {
      this.cacheDir = cacheDir;
      this.cleanupCacheDir = false;
    } else {
      final String userConfiguredCacheDir = System.getProperty(CACHE_DIR_PROPERTY, System.getenv(CACHE_DIR_ENV_VAR));
      if (userConfiguredCacheDir != null) {
        this.cacheDir = new File(userConfiguredCacheDir);
        this.cleanupCacheDir = false;
      } else {
        log.warn("No cache directory provided; benchmark data caching is disabled. "
                 + "Set the 'druid.benchmark.cacheDir' property or 'DRUID_BENCHMARK_CACHE_DIR' environment variable "
                 + "to use caching.");
        this.cacheDir = FileUtils.createTempDir();
        this.cleanupCacheDir = true;
      }
    }
  }

  public File getCacheDir()
  {
    return cacheDir;
  }


  public QueryableIndex generate(
      final DataSegment dataSegment,
      final GeneratorSchemaInfo schemaInfo,
      final Granularity granularity,
      final int numRows
  )
  {
    return generate(dataSegment, schemaInfo, schemaInfo.getDimensionsSpec(), TransformSpec.NONE, IndexSpec.DEFAULT, granularity, numRows);
  }

  public QueryableIndex generate(
      final DataSegment dataSegment,
      final GeneratorSchemaInfo schemaInfo,
      final IndexSpec indexSpec,
      final Granularity granularity,
      final int numRows
  )
  {
    return generate(dataSegment, schemaInfo, schemaInfo.getDimensionsSpec(), TransformSpec.NONE, indexSpec, granularity, numRows);
  }

  public QueryableIndex generate(
      final DataSegment dataSegment,
      final GeneratorSchemaInfo schemaInfo,
      final DimensionsSpec dimensionsSpec,
      final TransformSpec transformSpec,
      final IndexSpec indexSpec,
      final Granularity queryGranularity,
      final int numRows
  )
  {
    // In case we need to generate hyperUniques or json
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
    NestedDataModule.registerHandlersAndSerde();

    final String dataHash = Hashing.sha256()
                                   .newHasher()
                                   .putString(dataSegment.getId().toString(), StandardCharsets.UTF_8)
                                   .putString(schemaInfo.toString(), StandardCharsets.UTF_8)
                                   .putString(dimensionsSpec.toString(), StandardCharsets.UTF_8)
                                   .putString(queryGranularity.toString(), StandardCharsets.UTF_8)
                                   .putString(indexSpec.toString(), StandardCharsets.UTF_8)
                                   .putInt(numRows)
                                   .hash()
                                   .toString();

    final File outDir = new File(getSegmentDir(dataSegment.getId(), dataHash), "merged");

    if (outDir.exists()) {
      try {
        log.info("Found segment with hash[%s] cached in directory[%s].", dataHash, outDir);
        return TestHelper.getTestIndexIO().loadIndex(outDir);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    log.info("Writing segment with hash[%s] to directory[%s].", dataHash, outDir);

    final DataGenerator dataGenerator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        dataSegment.getId().hashCode(), /* Use segment identifier hashCode as seed */
        schemaInfo.getDataInterval(),
        numRows
    );

    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(dimensionsSpec)
        .withMetrics(schemaInfo.getAggsArray())
        .withRollup(schemaInfo.isWithRollup())
        .withQueryGranularity(queryGranularity)
        .build();

    final List<InputRow> rows = new ArrayList<>();
    final List<QueryableIndex> indexes = new ArrayList<>();

    Transformer transformer = transformSpec.toTransformer();
    InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec(null, null, null),
        dimensionsSpec,
        null
    );

    for (int i = 0; i < numRows; i++) {
      Map<String, Object> raw = dataGenerator.nextRaw();
      InputRow inputRow = MapInputRowParser.parse(rowSchema, raw);
      InputRow transformedRow = transformer.transform(inputRow);
      rows.add(transformedRow);

      if ((i + 1) % 20000 == 0) {
        log.info("%,d/%,d rows generated for[%s].", i + 1, numRows, dataSegment);
      }

      if (rows.size() % MAX_ROWS_IN_MEMORY == 0) {
        indexes.add(makeIndex(dataSegment.getId(), dataHash, indexes.size(), rows, indexSchema, indexSpec));
        rows.clear();
      }
    }

    log.info("%,d/%,d rows generated for[%s].", numRows, numRows, dataSegment);

    if (rows.size() > 0) {
      indexes.add(makeIndex(dataSegment.getId(), dataHash, indexes.size(), rows, indexSchema, indexSpec));
      rows.clear();
    }

    final QueryableIndex retVal;

    if (indexes.isEmpty()) {
      throw new ISE("No rows to index?");
    } else {
      try {
        retVal = TestHelper
            .getTestIndexIO()
            .loadIndex(
                TestHelper.getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                          .mergeQueryableIndex(
                              indexes,
                              false,
                              schemaInfo.getAggs()
                                        .stream()
                                        .map(AggregatorFactory::getCombiningFactory)
                                        .toArray(AggregatorFactory[]::new),
                              null,
                              outDir,
                              indexSpec,
                              indexSpec,
                              new BaseProgressIndicator(),
                              null,
                              -1
                          )
            );

        for (QueryableIndex index : indexes) {
          index.close();
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    log.info("Finished writing segment[%s] to[%s]", dataSegment, outDir);

    return retVal;
  }

  public IncrementalIndex generateIncrementalIndex(

      final DataSegment dataSegment,
      final GeneratorSchemaInfo schemaInfo,
      final Granularity granularity,
      final int numRows
  )
  {
    // In case we need to generate hyperUniques.
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    final String dataHash = Hashing.sha256()
                                   .newHasher()
                                   .putString(dataSegment.getId().toString(), StandardCharsets.UTF_8)
                                   .putString(schemaInfo.toString(), StandardCharsets.UTF_8)
                                   .putString(granularity.toString(), StandardCharsets.UTF_8)
                                   .putInt(numRows)
                                   .hash()
                                   .toString();


    final DataGenerator dataGenerator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        dataSegment.getId().hashCode(), /* Use segment identifier hashCode as seed */
        schemaInfo.getDataInterval(),
        numRows
    );

    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(schemaInfo.getDimensionsSpec())
        .withMetrics(schemaInfo.getAggsArray())
        .withRollup(schemaInfo.isWithRollup())
        .withQueryGranularity(granularity)
        .build();

    final List<InputRow> rows = new ArrayList<>();

    for (int i = 0; i < numRows; i++) {
      final InputRow row = dataGenerator.nextRow();
      rows.add(row);

      if ((i + 1) % 20000 == 0) {
        log.info("%,d/%,d rows generated for[%s].", i + 1, numRows, dataSegment);
      }
    }

    log.info("%,d/%,d rows generated for[%s].", numRows, numRows, dataSegment);

    return makeIncrementalIndex(dataSegment.getId(), dataHash, 0, rows, indexSchema);
  }

  @Override
  public void close() throws IOException
  {
    if (cleanupCacheDir) {
      FileUtils.deleteDirectory(cacheDir);
    }
  }

  private QueryableIndex makeIndex(
      final SegmentId identifier,
      final String dataHash,
      final int indexNumber,
      final List<InputRow> rows,
      final IncrementalIndexSchema indexSchema,
      final IndexSpec indexSpec
  )
  {
    return IndexBuilder
        .create()
        .schema(indexSchema)
        .indexSpec(indexSpec)
        .tmpDir(new File(getSegmentDir(identifier, dataHash), String.valueOf(indexNumber)))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .rows(rows)
        .buildMMappedIndex();
  }

  private IncrementalIndex makeIncrementalIndex(
      final SegmentId identifier,
      final String dataHash,
      final int indexNumber,
      final List<InputRow> rows,
      final IncrementalIndexSchema indexSchema
  )
  {
    return IndexBuilder
        .create()
        .schema(indexSchema)
        .tmpDir(new File(getSegmentDir(identifier, dataHash), String.valueOf(indexNumber)))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .rows(rows)
        .buildIncrementalIndex();
  }

  private File getSegmentDir(final SegmentId identifier, final String dataHash)
  {
    return new File(cacheDir, StringUtils.format("%s_%s", identifier, dataHash));
  }
}
