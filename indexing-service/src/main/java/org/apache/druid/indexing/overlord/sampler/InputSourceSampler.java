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

package org.apache.druid.indexing.overlord.sampler;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimedShutoffInputSourceReader;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse.SamplerResponseRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InputSourceSampler
{
  private static final String SAMPLER_DATA_SOURCE = "sampler";

  private static final DataSchema DEFAULT_DATA_SCHEMA = new DataSchema(
      SAMPLER_DATA_SOURCE,
      new TimestampSpec(null, null, null),
      new DimensionsSpec(null),
      null,
      null,
      null
  );

  // We want to be able to sort the list of processed results back into the same order that we read them from the
  // firehose so that the rows in the data loader are not always changing. To do this, we add a temporary column to the
  // InputRow (in SamplerInputRow) and tag each row with a sortKey. We use an aggregator so that it will not affect
  // rollup, and we use a longMin aggregator so that as rows get rolled up, the earlier rows stay stable and later
  // rows may get rolled into these rows. After getting the results back from the IncrementalIndex, we sort by this
  // column and then exclude it from the response.
  private static final AggregatorFactory INTERNAL_ORDERING_AGGREGATOR = new LongMinAggregatorFactory(
      SamplerInputRow.SAMPLER_ORDERING_COLUMN,
      SamplerInputRow.SAMPLER_ORDERING_COLUMN
  );

  public SamplerResponse sample(
      final InputSource inputSource,
      // inputFormat can be null only if inputSource.needsFormat() = false or parser is specified.
      @Nullable final InputFormat inputFormat,
      @Nullable final DataSchema dataSchema,
      @Nullable final SamplerConfig samplerConfig
  )
  {
    Preconditions.checkNotNull(inputSource, "inputSource required");
    if (inputSource.needsFormat()) {
      Preconditions.checkNotNull(inputFormat, "inputFormat required");
    }
    final DataSchema nonNullDataSchema = dataSchema == null
                                         ? DEFAULT_DATA_SCHEMA
                                         : dataSchema;
    final SamplerConfig nonNullSamplerConfig = samplerConfig == null
                                               ? SamplerConfig.empty()
                                               : samplerConfig;

    final Closer closer = Closer.create();
    final File tempDir = FileUtils.createTempDir();
    closer.register(() -> FileUtils.deleteDirectory(tempDir));

    final InputSourceReader reader = buildReader(
        nonNullSamplerConfig,
        nonNullDataSchema,
        inputSource,
        inputFormat,
        tempDir
    );
    try (final CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample();
         final IncrementalIndex<Aggregator> index = buildIncrementalIndex(nonNullSamplerConfig, nonNullDataSchema);
         final Closer closer1 = closer) {
      SamplerResponseRow[] responseRows = new SamplerResponseRow[nonNullSamplerConfig.getNumRows()];
      int counter = 0, numRowsIndexed = 0;

      while (counter < responseRows.length && iterator.hasNext()) {
        Map<String, Object> rawColumns = null;
        try {
          final InputRowListPlusRawValues inputRowListPlusRawValues = iterator.next();

          if (inputRowListPlusRawValues.getRawValues() != null) {
            rawColumns = inputRowListPlusRawValues.getRawValues();
          }

          if (inputRowListPlusRawValues.getParseException() != null) {
            throw inputRowListPlusRawValues.getParseException();
          }

          if (inputRowListPlusRawValues.getInputRows() == null) {
            continue;
          }

          for (InputRow row : inputRowListPlusRawValues.getInputRows()) {
            if (!Intervals.ETERNITY.contains(row.getTimestamp())) {
              throw new ParseException("Timestamp cannot be represented as a long: [%s]", row);
            }
            IncrementalIndexAddResult result = index.add(new SamplerInputRow(row, counter), true);
            if (result.getParseException() != null) {
              throw result.getParseException();
            } else {
              // store the raw value; will be merged with the data from the IncrementalIndex later
              responseRows[counter] = new SamplerResponseRow(rawColumns, null, null, null);
              counter++;
              numRowsIndexed++;
            }
          }
        }
        catch (ParseException e) {
          responseRows[counter] = new SamplerResponseRow(rawColumns, null, true, e.getMessage());
          counter++;
        }
      }

      final List<String> columnNames = index.getColumnNames();
      columnNames.remove(SamplerInputRow.SAMPLER_ORDERING_COLUMN);

      for (Row row : index) {
        Map<String, Object> parsed = new HashMap<>();

        columnNames.forEach(k -> parsed.put(k, row.getRaw(k)));
        parsed.put(ColumnHolder.TIME_COLUMN_NAME, row.getTimestampFromEpoch());

        Number sortKey = row.getMetric(SamplerInputRow.SAMPLER_ORDERING_COLUMN);
        if (sortKey != null) {
          responseRows[sortKey.intValue()] = responseRows[sortKey.intValue()].withParsed(parsed);
        }
      }

      return new SamplerResponse(
          counter,
          numRowsIndexed,
          Arrays.stream(responseRows)
                .filter(Objects::nonNull)
                .filter(x -> x.getParsed() != null || x.isUnparseable() != null)
                .collect(Collectors.toList())
      );
    }
    catch (Exception e) {
      throw new SamplerException(e, "Failed to sample data: %s", e.getMessage());
    }
  }

  private InputSourceReader buildReader(
      SamplerConfig samplerConfig,
      DataSchema dataSchema,
      InputSource inputSource,
      @Nullable InputFormat inputFormat,
      File tempDir
  )
  {
    final List<String> metricsNames = Arrays.stream(dataSchema.getAggregators())
                                            .map(AggregatorFactory::getName)
                                            .collect(Collectors.toList());
    final InputRowSchema inputRowSchema = new InputRowSchema(
        dataSchema.getTimestampSpec(),
        dataSchema.getDimensionsSpec(),
        metricsNames
    );

    InputSourceReader reader = inputSource.reader(inputRowSchema, inputFormat, tempDir);

    if (samplerConfig.getTimeoutMs() > 0) {
      reader = new TimedShutoffInputSourceReader(reader, DateTimes.nowUtc().plusMillis(samplerConfig.getTimeoutMs()));
    }

    return dataSchema.getTransformSpec().decorate(reader);
  }

  private IncrementalIndex<Aggregator> buildIncrementalIndex(SamplerConfig samplerConfig, DataSchema dataSchema)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withTimestampSpec(dataSchema.getTimestampSpec())
        .withQueryGranularity(dataSchema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(dataSchema.getDimensionsSpec())
        .withMetrics(ArrayUtils.addAll(dataSchema.getAggregators(), INTERNAL_ORDERING_AGGREGATOR))
        .withRollup(dataSchema.getGranularitySpec().isRollup())
        .build();

    return new IncrementalIndex.Builder().setIndexSchema(schema)
                                         .setMaxRowCount(samplerConfig.getNumRows())
                                         .buildOnheap();
  }
}
