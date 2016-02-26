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

package io.druid.indexing.overlord;

import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.TaskLocation;

import java.util.concurrent.Executor;

public class TaskRunnerUtils
{
  private static final EmittingLogger log = new EmittingLogger(TaskRunnerUtils.class);

  public static void notifyLocationChanged(
      final Iterable<Pair<TaskRunnerListener, Executor>> listeners,
      final String taskId,
      final TaskLocation location
  )
  {
    log.info("Task [%s] location changed to [%s].", taskId, location);
    for (final Pair<TaskRunnerListener, Executor> listener : listeners) {
      try {
        listener.rhs.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                listener.lhs.locationChanged(taskId, location);
              }
            }
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to notify task listener")
           .addData("taskId", taskId)
           .addData("taskLocation", location)
           .addData("listener", listener.toString())
           .emit();
      }
    }
  }
}
