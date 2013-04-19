package com.metamx.druid.realtime;

import com.metamx.druid.input.InputRow;
import org.joda.time.DateTime;

import java.io.IOException;

public class MinTimeFirehose implements Firehose
{
  private final Firehose firehose;
  private final DateTime minTime;

  public MinTimeFirehose(Firehose firehose, DateTime minTime)
  {
    this.firehose = firehose;
    this.minTime = minTime;
  }

  @Override
  public boolean hasMore()
  {
    return firehose.hasMore();
  }

  @Override
  public InputRow nextRow()
  {
    while (true) {
      final InputRow row = firehose.nextRow();
      if (row.getTimestampFromEpoch() >= minTime.getMillis()) {
        return row;
      }
    }
  }

  @Override
  public Runnable commit()
  {
    return firehose.commit();
  }

  @Override
  public void close() throws IOException
  {
    firehose.close();
  }
}
