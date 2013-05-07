package com.metamx.druid.realtime.firehose;

import com.google.common.base.Predicate;
import com.metamx.druid.input.InputRow;

import java.io.IOException;

/**
 * Provides a view on a firehose that only returns rows that match a certain predicate.
 * Not thread-safe.
 */
public class PredicateFirehose implements Firehose
{
  private final Firehose firehose;
  private final Predicate<InputRow> predicate;

  private InputRow savedInputRow = null;

  public PredicateFirehose(Firehose firehose, Predicate<InputRow> predicate)
  {
    this.firehose = firehose;
    this.predicate = predicate;
  }

  @Override
  public boolean hasMore()
  {
    if (savedInputRow != null) {
      return true;
    }

    while (firehose.hasMore()) {
      final InputRow row = firehose.nextRow();
      if (predicate.apply(row)) {
        savedInputRow = row;
        return true;
      }
    }

    return false;
  }

  @Override
  public InputRow nextRow()
  {
    final InputRow row = savedInputRow;
    savedInputRow = null;
    return row;
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
