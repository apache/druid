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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.AbstractTextFilesFirehoseFactory;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FirehoseSampler
{
  private static final EmittingLogger log = new EmittingLogger(FirehoseSampler.class);

  // These are convenience shims to allow the data loader to not need to provide a dummy parseSpec during the early
  // stages when the parameters for the parseSpec are still unknown and they are only interested in the unparsed rows.
  // We need two of these because firehose factories based on AbstractTextFilesFirehoseFactory expect to be used with
  // StringInputRowParser, while all the others expect InputRowParser.
  // ---------------------------
  private static final InputRowParser EMPTY_STRING_PARSER_SHIM = new StringInputRowParser(
      new ParseSpec(new TimestampSpec(null, null, DateTimes.EPOCH), new DimensionsSpec(null))
      {
        @Override
        public Parser<String, Object> makeParser()
        {
          return new Parser<String, Object>()
          {
            @Nullable
            @Override
            public Map<String, Object> parseToMap(String input)
            {
              throw new ParseException(null);
            }

            @Override
            public void setFieldNames(Iterable<String> fieldNames)
            {
            }

            @Override
            public List<String> getFieldNames()
            {
              return ImmutableList.of();
            }
          };
        }
      }, null);

  private static final InputRowParser EMPTY_PARSER_SHIM = new InputRowParser()
  {
    @Override
    public ParseSpec getParseSpec()
    {
      return null;
    }

    @Override
    public InputRowParser withParseSpec(ParseSpec parseSpec)
    {
      return null;
    }

    @Override
    public List<InputRow> parseBatch(Object input)
    {
      throw new ParseException(null);
    }

    @Override
    public InputRow parse(Object input)
    {
      throw new ParseException(null);
    }
  };
  // ---------------------------

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

  private final ObjectMapper objectMapper;
  private final SamplerCache samplerCache;

  @Inject
  public FirehoseSampler(ObjectMapper objectMapper, SamplerCache samplerCache)
  {
    this.objectMapper = objectMapper;
    this.samplerCache = samplerCache;
  }

  public SamplerResponse sample(FirehoseFactory firehoseFactory, DataSchema dataSchema, SamplerConfig samplerConfig)
  {
    Preconditions.checkNotNull(firehoseFactory, "firehoseFactory required");

    if (dataSchema == null) {
      dataSchema = new DataSchema("sampler", null, null, null, null, objectMapper);
    }

    if (samplerConfig == null) {
      samplerConfig = SamplerConfig.empty();
    }

    final InputRowParser parser = dataSchema.getParser() != null
                                  ? dataSchema.getParser()
                                  : (firehoseFactory instanceof AbstractTextFilesFirehoseFactory
                                     ? EMPTY_STRING_PARSER_SHIM
                                     : EMPTY_PARSER_SHIM);

    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withTimestampSpec(parser)
        .withQueryGranularity(dataSchema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(parser)
        .withMetrics(ArrayUtils.addAll(dataSchema.getAggregators(), INTERNAL_ORDERING_AGGREGATOR))
        .withRollup(dataSchema.getGranularitySpec().isRollup())
        .build();

    FirehoseFactory myFirehoseFactory = null;
    boolean usingCachedData = true;
    if (!samplerConfig.isSkipCache() && samplerConfig.getCacheKey() != null) {
      myFirehoseFactory = samplerCache.getAsFirehoseFactory(samplerConfig.getCacheKey(), parser);
    }
    if (myFirehoseFactory == null) {
      myFirehoseFactory = firehoseFactory;
      usingCachedData = false;
    }

    if (samplerConfig.getTimeoutMs() > 0) {
      myFirehoseFactory = new TimedShutoffFirehoseFactory(
          myFirehoseFactory,
          DateTimes.nowUtc().plusMillis(samplerConfig.getTimeoutMs())
      );
    }

    final File tempDir = Files.createTempDir();
    try (final Firehose firehose = myFirehoseFactory.connectForSampler(parser, tempDir);
         final IncrementalIndex index = new IncrementalIndex.Builder().setIndexSchema(indexSchema)
                                                                      .setMaxRowCount(samplerConfig.getNumRows())
                                                                      .buildOnheap()) {

      List<byte[]> dataToCache = new ArrayList<>();
      SamplerResponse.SamplerResponseRow responseRows[] = new SamplerResponse.SamplerResponseRow[samplerConfig.getNumRows()];
      int counter = 0, numRowsIndexed = 0;

      while (counter < responseRows.length && firehose.hasMore()) {
        String raw = null;
        try {
          final InputRowPlusRaw row = firehose.nextRowWithRaw();

          if (row == null || row.isEmpty()) {
            continue;
          }

          if (row.getRaw() != null) {
            raw = StringUtils.fromUtf8(row.getRaw());

            if (!usingCachedData) {
              dataToCache.add(row.getRaw());
            }
          }

          if (row.getParseException() != null) {
            throw row.getParseException();
          }

          if (row.getInputRow() == null) {
            continue;
          }

          if (!Intervals.ETERNITY.contains(row.getInputRow().getTimestamp())) {
            throw new ParseException("Timestamp cannot be represented as a long: [%s]", row.getInputRow());
          }

          IncrementalIndexAddResult result = index.add(new SamplerInputRow(row.getInputRow(), counter), true);
          if (result.getParseException() != null) {
            throw result.getParseException();
          } else {
            // store the raw value; will be merged with the data from the IncrementalIndex later
            responseRows[counter] = new SamplerResponse.SamplerResponseRow(raw, null, null, null);
            counter++;
            numRowsIndexed++;
          }
        }
        catch (ParseException e) {
          responseRows[counter] = new SamplerResponse.SamplerResponseRow(raw, null, true, e.getMessage());
          counter++;
        }
      }

      final List<String> columnNames = index.getColumnNames();
      columnNames.remove(SamplerInputRow.SAMPLER_ORDERING_COLUMN);

      for (Row row : (Iterable<Row>) index) {
        Map<String, Object> parsed = new HashMap<>();

        columnNames.forEach(k -> {
          if (row.getRaw(k) != null) {
            parsed.put(k, row.getRaw(k));
          }
        });
        parsed.put(ColumnHolder.TIME_COLUMN_NAME, row.getTimestampFromEpoch());

        Number sortKey = row.getMetric(SamplerInputRow.SAMPLER_ORDERING_COLUMN);
        if (sortKey != null) {
          responseRows[sortKey.intValue()] = responseRows[sortKey.intValue()].withParsed(parsed);
        }
      }

      // cache raw data if available
      String cacheKey = usingCachedData ? samplerConfig.getCacheKey() : null;
      if (!samplerConfig.isSkipCache() && !dataToCache.isEmpty()) {
        cacheKey = samplerCache.put(UUIDUtils.generateUuid(), dataToCache);
      }

      return new SamplerResponse(
          cacheKey,
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
    finally {
      try {
        FileUtils.deleteDirectory(tempDir);
      }
      catch (IOException e) {
        log.warn(e, "Failed to cleanup temporary directory");
      }
    }
  }
}
