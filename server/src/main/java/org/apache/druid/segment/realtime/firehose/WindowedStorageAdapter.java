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

import org.apache.druid.segment.StorageAdapter;
import org.joda.time.Interval;

public class WindowedStorageAdapter
{
  private final StorageAdapter adapter;
  private final Interval interval;

  public WindowedStorageAdapter(StorageAdapter adapter, Interval interval)
  {
    this.adapter = adapter;
    this.interval = interval;
  }

  public StorageAdapter getAdapter()
  {
    return adapter;
  }

  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public String toString()
  {
    return "WindowedStorageAdapter{" +
           "adapter=" + adapter +
           ", interval=" + interval +
           '}';
  }
}
