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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;

import java.io.File;
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
  public Firehose connect(InputRowParser parser, File temporaryDirectory) throws IOException
  {
    return new CombiningFirehose(parser, temporaryDirectory);
  }

  @JsonProperty("delegates")
  public List<FirehoseFactory> getDelegateFactoryList()
  {
    return delegateFactoryList;
  }

  class CombiningFirehose implements Firehose
  {
    private final InputRowParser parser;
    private final File temporaryDirectory;
    private final Iterator<FirehoseFactory> firehoseFactoryIterator;
    private volatile Firehose currentFirehose;

    CombiningFirehose(InputRowParser parser, File temporaryDirectory) throws IOException
    {
      this.firehoseFactoryIterator = delegateFactoryList.iterator();
      this.parser = parser;
      this.temporaryDirectory = temporaryDirectory;
      nextFirehose();
    }

    private void nextFirehose()
    {
      if (firehoseFactoryIterator.hasNext()) {
        try {
          if (currentFirehose != null) {
            currentFirehose.close();
          }

          currentFirehose = firehoseFactoryIterator.next().connect(parser, temporaryDirectory);
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
