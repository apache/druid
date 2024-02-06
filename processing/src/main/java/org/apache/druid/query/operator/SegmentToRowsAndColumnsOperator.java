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

package org.apache.druid.query.operator;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.Segment;

import java.io.Closeable;
import java.io.IOException;

public class SegmentToRowsAndColumnsOperator implements Operator
{
  private final Segment segment;

  public SegmentToRowsAndColumnsOperator(
      Segment segment
  )
  {
    this.segment = segment;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    try (final CloseableShapeshifter shifty = segment.as(CloseableShapeshifter.class)) {
      if (shifty == null) {
        throw DruidException.defensive("Segment [%s] cannot shapeshift", segment.asString());
      }
      RowsAndColumns rac;
      if (shifty instanceof RowsAndColumns) {
        rac = (RowsAndColumns) shifty;
      } else {
        rac = shifty.as(RowsAndColumns.class);
      }

      if (rac == null) {
        throw new ISE("Cannot work with segment of type[%s]", segment.getClass());
      }

      // After pushing in a single object, we are done, so ignore the signal and call completed()
      receiver.push(rac);
      receiver.completed();
    }
    catch (IOException e) {
      throw new RE(e, "Problem closing resources for segment[%s]", segment.getId());
    }
    return null;
  }
}
