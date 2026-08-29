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

package org.apache.druid.msq.input;

import org.apache.druid.segment.RowCountInspector;
import org.apache.druid.segment.Segment;

public class LoadableSegmentUtils
{
  /**
   * Gets the number of rows for a segment, using a {@link RowCountInspector}. Returns 0 when unknown.
   */
  public static int getSegmentRowCount(final Segment segment)
  {
    final RowCountInspector inspector = segment.as(RowCountInspector.class);
    return inspector != null ? inspector.getNumRows() : 0;
  }
}
