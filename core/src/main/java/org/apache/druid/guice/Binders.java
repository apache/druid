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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.tasklogs.TaskLogs;

/**
 */
@PublicApi
public class Binders
{

  public static MapBinder<String, DataSegmentKiller> dataSegmentKillerBinder(Binder binder)
  {
    return MapBinder.newMapBinder(binder, String.class, DataSegmentKiller.class);
  }

  public static MapBinder<String, DataSegmentMover> dataSegmentMoverBinder(Binder binder)
  {
    return MapBinder.newMapBinder(binder, String.class, DataSegmentMover.class);
  }

  public static MapBinder<String, DataSegmentArchiver> dataSegmentArchiverBinder(Binder binder)
  {
    return MapBinder.newMapBinder(binder, String.class, DataSegmentArchiver.class);
  }

  public static MapBinder<String, DataSegmentPusher> dataSegmentPusherBinder(Binder binder)
  {
    return PolyBind.optionBinder(binder, Key.get(DataSegmentPusher.class));
  }

  public static MapBinder<String, TaskLogs> taskLogsBinder(Binder binder)
  {
    return PolyBind.optionBinder(binder, Key.get(TaskLogs.class));
  }
}
