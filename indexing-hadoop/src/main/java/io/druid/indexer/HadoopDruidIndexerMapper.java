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

package io.druid.indexer;

import com.google.common.collect.ImmutableList;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.collect.Utils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopDruidIndexerMapper.class);

  protected HadoopDruidIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;
  private boolean reportParseExceptions;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();
    reportParseExceptions = !config.isIgnoreInvalidRows();
  }

  public HadoopDruidIndexerConfig getConfig()
  {
    return config;
  }

  @Override
  protected void map(Object key, Object value, Context context) throws IOException, InterruptedException
  {
    try {
      final List<InputRow> inputRows;
      try {
        inputRows = parseInputRow(value, parser);
      }
      catch (ParseException e) {
        if (reportParseExceptions) {
          throw e;
        }
        log.debug(e, "Ignoring invalid row [%s] due to parsing error", value);
        context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
        return; // we're ignoring this invalid row
      }

      for (InputRow inputRow : inputRows) {
        if (inputRow == null) {
          // Throw away null rows from the parser.
          log.debug("Throwing away row [%s]", value);
          continue;
        }
        if (!granularitySpec.bucketIntervals().isPresent()
            || granularitySpec.bucketInterval(DateTimes.utc(inputRow.getTimestampFromEpoch()))
                              .isPresent()) {
          innerMap(inputRow, context, reportParseExceptions);
        }
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  private static List<InputRow> parseInputRow(Object value, InputRowParser parser)
  {
    if (parser instanceof StringInputRowParser && value instanceof Text) {
      //Note: This is to ensure backward compatibility with 0.7.0 and before
      //HadoopyStringInputRowParser can handle this and this special case is not needed
      //except for backward compatibility
      return Utils.nullableListOf(((StringInputRowParser) parser).parse(value.toString()));
    } else if (value instanceof InputRow) {
      return ImmutableList.of((InputRow) value);
    } else if (value == null) {
      // Pass through nulls so they get thrown away.
      return Utils.nullableListOf((InputRow) null);
    } else {
      return parser.parseBatch(value);
    }
  }

  abstract protected void innerMap(InputRow inputRow, Context context, boolean reportParseExceptions)
      throws IOException, InterruptedException;

}
