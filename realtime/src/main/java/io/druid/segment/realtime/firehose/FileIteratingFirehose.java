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
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 */
public class FileIteratingFirehose<T> implements Firehose
{
  private final LineIteratorFactory<T> lineIteratorFactory;
  private final Queue<T> objectQueue;
  private final StringInputRowParser parser;

  private LineIterator lineIterator = null;

  public FileIteratingFirehose(
      LineIteratorFactory lineIteratorFactory,
      Queue<T> objectQueue,
      StringInputRowParser parser
  )
  {
    this.lineIteratorFactory = lineIteratorFactory;
    this.objectQueue = objectQueue;
    this.parser = parser;
  }

  @Override
  public boolean hasMore()
  {
    try {
      nextFile();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return lineIterator != null && lineIterator.hasNext();
  }

  @Override
  public InputRow nextRow()
  {
    try {
      nextFile();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    if (lineIterator == null) {
      throw new NoSuchElementException();
    }

    return parser.parse(lineIterator.next());
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    objectQueue.clear();
    if (lineIterator != null) {
      lineIterator.close();
    }
  }

  // Rolls over our streams and iterators to the next file, if appropriate
  private void nextFile() throws Exception
  {

    if (lineIterator == null || !lineIterator.hasNext()) {

      // Close old streams, maybe.
      if (lineIterator != null) {
        lineIterator.close();
      }

      // Open new streams, maybe.
      final T nextObj = objectQueue.poll();
      if (nextObj != null) {

        try {
          lineIterator = lineIteratorFactory.make(nextObj);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}
