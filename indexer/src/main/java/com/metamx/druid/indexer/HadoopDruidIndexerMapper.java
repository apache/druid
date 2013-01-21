package com.metamx.druid.indexer;

import com.metamx.common.RE;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.input.InputRow;
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
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
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
      catch (IllegalArgumentException e) {
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
