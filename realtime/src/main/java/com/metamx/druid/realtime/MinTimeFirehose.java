package com.metamx.druid.realtime;

import com.metamx.druid.input.InputRow;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Provides a view on a firehose that only returns rows at or after a certain minimum timestamp.
 * Not thread-safe.
 */
public class MinTimeFirehose implements Firehose
{
  private final Firehose firehose;
  private final DateTime minTime;

  private InputRow savedInputRow = null;

  public MinTimeFirehose(Firehose firehose, DateTime minTime)
  {
    this.firehose = firehose;
    this.minTime = minTime;
  }

  @Override
  public boolean hasMore()
  {
    if (savedInputRow != null) {
      return true;
    }

    while (firehose.hasMore()) {
      final InputRow row = firehose.nextRow();
      if (acceptable(row)) {
        savedInputRow = row;
        return true;
      }
    }

    return false;
  }

  @Override
  public InputRow nextRow()
  {
    if (savedInputRow != null) {
      final InputRow row = savedInputRow;
      savedInputRow = null;
      return row;
    } else {
      while (firehose.hasMore()) {
        final InputRow row = firehose.nextRow();
        if (acceptable(row)) {
          return row;
        }
      }

      throw new NoSuchElementException("No more rows!");
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

  private boolean acceptable(InputRow row)
  {
    return row.getTimestampFromEpoch() >= minTime.getMillis();
  }
}
