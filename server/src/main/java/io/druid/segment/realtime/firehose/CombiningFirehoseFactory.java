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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Creates firehose that combines data from different Firehoses. Useful for ingesting data from multiple sources.
 */
public class CombiningFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(CombiningFirehoseFactory.class);

  private final List<FirehoseFactory> delegateFactoryList;

  @JsonCreator
  public CombiningFirehoseFactory(
      @JsonProperty("delegates") List<FirehoseFactory> delegateFactoryList
  )
  {
    Preconditions.checkArgument(!delegateFactoryList.isEmpty());
    this.delegateFactoryList = delegateFactoryList;
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException
  {
    return new CombiningFirehose(parser);
  }

  @Override
  public InputRowParser getParser()
  {
    return delegateFactoryList.get(0).getParser();
  }

  @JsonProperty("delegates")
  public List<FirehoseFactory> getDelegateFactoryList()
  {
    return delegateFactoryList;
  }

  public class CombiningFirehose implements Firehose
  {
    private final InputRowParser parser;
    private final Iterator<FirehoseFactory> firehoseFactoryIterator;
    private volatile Firehose currentFirehose;

    public CombiningFirehose(InputRowParser parser) throws IOException
    {
      this.firehoseFactoryIterator = delegateFactoryList.iterator();
      this.parser = parser;
      nextFirehose();
    }

    private void nextFirehose()
    {
      if (firehoseFactoryIterator.hasNext()) {
        try {
          if (currentFirehose != null) {
            currentFirehose.close();
          }

          currentFirehose = firehoseFactoryIterator.next().connect(parser);
        }
        catch (IOException e) {
          if (currentFirehose != null) {
            try {
              currentFirehose.close();
            }
            catch (IOException e2) {
              log.error(e, "Unable to close currentFirehose!");
              throw Throwables.propagate(e2);
            }
          }
          throw Throwables.propagate(e);
        }
      }
    }

    @Override
    public boolean hasMore()
    {
      return currentFirehose.hasMore();
    }

    @Override
    public InputRow nextRow()
    {
      InputRow rv = currentFirehose.nextRow();
      if (!currentFirehose.hasMore()) {
        nextFirehose();
      }
      return rv;
    }

    @Override
    public Runnable commit()
    {
      return currentFirehose.commit();
    }

    @Override
    public void close() throws IOException
    {
      currentFirehose.close();
    }
  }
}
