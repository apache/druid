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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Supplier;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseV2;

public class Committers
{
  private static final Committer NIL = new Committer()
  {
    @Override
    public Object getMetadata()
    {
      return null;
    }

    @Override
    public void run()
    {
      // Do nothing
    }
  };

  public static Supplier<Committer> supplierFromRunnable(final Runnable runnable)
  {
    return supplierOf(
        new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return null;
          }

          @Override
          public void run()
          {
            runnable.run();
          }
        }
    );
  }

  public static Supplier<Committer> supplierFromFirehose(final Firehose firehose)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        final Runnable commitRunnable = firehose.commit();
        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return null;
          }

          @Override
          public void run()
          {
            commitRunnable.run();
          }
        };
      }
    };
  }

  public static Supplier<Committer> supplierFromFirehoseV2(final FirehoseV2 firehose)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        return firehose.makeCommitter();
      }
    };
  }

  public static Committer nil()
  {
    return NIL;
  }

  public static Supplier<Committer> supplierOf(final Committer committer)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        return committer;
      }
    };
  }
}
