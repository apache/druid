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

import com.google.common.base.Throwables;
import io.druid.common.guava.Runnables;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.StringInputRowParser;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 */
public class FileIteratingFirehose implements Firehose
{
  private final Iterator<LineIterator> lineIterators;
  private final StringInputRowParser parser;

  private LineIterator lineIterator = null;

  public FileIteratingFirehose(
      Iterator<LineIterator> lineIterators,
      StringInputRowParser parser
  )
  {
    this.lineIterators = lineIterators;
    this.parser = parser;
  }

  @Override
  public boolean hasMore()
  {
    try {
      return lineIterators.hasNext() || (lineIterator != null && lineIterator.hasNext());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public InputRow nextRow()
  {
    try {
      if (lineIterator == null || !lineIterator.hasNext()) {
        // Close old streams, maybe.
        if (lineIterator != null) {
          lineIterator.close();
        }

        lineIterator = lineIterators.next();
      }

      return parser.parse(lineIterator.next());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    if (lineIterator != null) {
      lineIterator.close();
    }
  }
}
