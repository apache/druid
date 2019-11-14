/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
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
  public Firehose connect(InputRowParser parser, File temporaryDirectory)
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

    CombiningFirehose(InputRowParser parser, File temporaryDirectory)
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
              throw new RuntimeException(e2);
            }
          }
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public boolean hasMore() throws IOException
    {
      return currentFirehose.hasMore();
    }

    @Nullable
    @Override
    public InputRow nextRow() throws IOException
    {
      InputRow rv = currentFirehose.nextRow();
      if (!currentFirehose.hasMore()) {
        nextFirehose();
      }
      return rv;
    }

    @Override
    public void close() throws IOException
    {
      currentFirehose.close();
    }
  }
}
