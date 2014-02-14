/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer;

import com.metamx.common.RE;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<LongWritable, Text, KEYOUT, VALUEOUT>
{
  private HadoopDruidIndexerConfig config;
  private StringInputRowParser parser;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfigBuilder.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
  }

  public HadoopDruidIndexerConfig getConfig()
  {
    return config;
  }

  public StringInputRowParser getParser()
  {
    return parser;
  }

  @Override
  protected void map(
      LongWritable key, Text value, Context context
  ) throws IOException, InterruptedException
  {
    try {
      final InputRow inputRow;
      try {
        inputRow = parser.parse(value.toString());
      }
      catch (Exception e) {
        if (config.isIgnoreInvalidRows()) {
          context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
          return; // we're ignoring this invalid row
        } else {
          throw e;
        }
      }

      if(config.getGranularitySpec().bucketInterval(new DateTime(inputRow.getTimestampFromEpoch())).isPresent()) {
        innerMap(inputRow, value, context);
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  abstract protected void innerMap(InputRow inputRow, Text text, Context context)
      throws IOException, InterruptedException;
}
