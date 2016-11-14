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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.RE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopDruidIndexerMapper.class);

  protected HadoopDruidIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;
  private boolean reportParseExceptions;
  private AdditionalDimsMergerFactory.Merger dimsMerger;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();
    reportParseExceptions = !config.isIgnoreInvalidRows();

    dimsMerger = new AdditionalDimsMergerFactory().create(config.getPathSpec().additionalDimValues(context));
  }

  public HadoopDruidIndexerConfig getConfig()
  {
    return config;
  }

  public InputRowParser getParser()
  {
    return parser;
  }

  @Override
  protected void map(
      Object key, Object value, Context context
  ) throws IOException, InterruptedException
  {
    try {
      final InputRow inputRow;
      try {
        inputRow = dimsMerger.merge(parseInputRow(value, parser));
      }
      catch (ParseException e) {
        if (reportParseExceptions) {
          throw e;
        }
        log.debug(e, "Ignoring invalid row [%s] due to parsing error", value.toString());
        context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
        return; // we're ignoring this invalid row

      }

      if (!granularitySpec.bucketIntervals().isPresent()
          || granularitySpec.bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()))
                            .isPresent()) {
        innerMap(inputRow, value, context, reportParseExceptions);
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  public final static InputRow parseInputRow(Object value, InputRowParser parser)
  {
    if (parser instanceof StringInputRowParser && value instanceof Text) {
      //Note: This is to ensure backward compatibility with 0.7.0 and before
      //HadoopyStringInputRowParser can handle this and this special case is not needed
      //except for backward compatibility
      return ((StringInputRowParser) parser).parse(value.toString());
    } else if (value instanceof InputRow) {
      return (InputRow) value;
    } else {
      return parser.parse(value);
    }
  }

  abstract protected void innerMap(InputRow inputRow, Object value, Context context, boolean reportParseExceptions)
      throws IOException, InterruptedException;

  private class AdditionalDimsMergerFactory
  {
    public Merger create(
        Map<String, String> additionalDimValues
    )
    {
      if (additionalDimValues != null && additionalDimValues.size() > 0) {
        return new Merger(additionalDimValues);
      } else {
        return new Merger(null) {
          @Override
          public InputRow merge(InputRow inputRow)
          {
            return inputRow;
          }
        };
      }
    }

    private class Merger
    {
      private final Map<String, String> additionalDimValues;
      private List<String> mergedDims;

      public Merger(
          Map<String, String> additionalDimValues
      )
      {
        this.additionalDimValues = additionalDimValues;
      }

      public InputRow merge(
          InputRow inputRow
      )
      {
        if (isMapBasedInputRow(inputRow)) {
          MapBasedInputRow mapBasedInputRow = (MapBasedInputRow)inputRow;
          Map<String, Object> eventMap = Maps.newHashMap(mapBasedInputRow.getEvent());
          eventMap.putAll(additionalDimValues);

          return new MapBasedInputRow(
              mapBasedInputRow.getTimestamp(),
              getMergedDims(mapBasedInputRow),
              eventMap
          );
        }

        return inputRow;
      }

      private List<String> getMergedDims(MapBasedInputRow mapBasedInputRow)
      {
        if (mergedDims == null) {
          List<String> orgDimensions = mapBasedInputRow.getDimensions();
          Set<String> additionalDims = additionalDimValues.keySet();
          mergedDims = Lists.newArrayListWithCapacity(orgDimensions.size() + additionalDims.size());
          mergedDims.addAll(orgDimensions);
          for (String partitionDimension : additionalDims) {
            if (!mergedDims.contains(partitionDimension)) {
              mergedDims.add(partitionDimension);
            }
          }
        }

        return mergedDims;
      }

      private boolean isMapBasedInputRow(
          InputRow inputRow
      )
      {
        return inputRow instanceof MapBasedInputRow;
      }
    }
  }
}
