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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Predicate;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;

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
