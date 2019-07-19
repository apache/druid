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

package org.apache.druid.segment.realtime.plumber;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.Firehose;

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
    final Committer committer = new Committer()
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
    };
    return Suppliers.ofInstance(committer);
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

  public static Committer nil()
  {
    return NIL;
  }
}
