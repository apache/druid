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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import org.apache.druid.java.util.common.ISE;

public final class DoublesSketches
{
  /**
   * Runs the given task that updates a Doubles sketch backed by a direct byte buffer. This method intentionally
   * accepts the update task as a {@link Runnable} instead of accpeting parameters of a sketch and other values
   * needed for the update. This is to avoid any potential performance impact especially when the sketch is updated
   * in a tight loop. The update task can throw NullPointerException because of the known issue filed in
   * https://github.com/apache/druid/issues/11544. This method catches NPE and converts it to a more user-friendly
   * exception. This method should be removed once the known bug above is fixed.
   */
  public static void handleMaxStreamLengthLimit(Runnable updateSketchTask)
  {
    try {
      updateSketchTask.run();
    }
    catch (NullPointerException e) {
      throw new ISE(
          e,
          "NullPointerException was thrown while updating Doubles sketch. "
          + "This exception could be potentially because of the known bug filed in https://github.com/apache/druid/issues/11544. "
          + "You could try a higher maxStreamLength than current to work around this bug if that is the case. "
          + "See https://druid.apache.org/docs/latest/development/extensions-core/datasketches-quantiles.html for more details."
      );
    }
  }

  private DoublesSketches()
  {
  }
}
