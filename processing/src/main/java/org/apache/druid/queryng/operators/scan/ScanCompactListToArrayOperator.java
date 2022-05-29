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

package org.apache.druid.queryng.operators.scan;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.List;

/**
 * Converts individual scan query rows with the
 * {@link org.apache.druid.query.scan.ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST
 * ResultFormat.RESULT_FORMAT_COMPACTED_LIST} format into an object array with fields
 * in the order given by the output schema.
 *
 * @See {@link org.apache.druid.query.scan.ScanQueryQueryToolChest.resultsAsArrays
 * ScanQueryQueryToolChest.resultsAsArrays}
 */
public class ScanCompactListToArrayOperator extends MappingOperator<List<Object>, Object[]>
{
  private final List<String> fields;
  private int rowCount;

  public ScanCompactListToArrayOperator(
      FragmentContext context,
      Operator<List<Object>> input,
      List<String> fields)
  {
    super(context, input);
    this.fields = fields;
  }

  @Override
  public Object[] next() throws ResultIterator.EofException
  {
    List<Object> row = inputIter.next();
    rowCount++;
    if (row.size() == fields.size()) {
      return row.toArray();
    } else if (fields.isEmpty()) {
      return new Object[0];
    } else {
      // Uh oh... mismatch in expected and actual field count. I don't think this should happen, so let's
      // throw an exception. If this really does happen, and there's a good reason for it, then we should remap
      // the result row here.
      throw new ISE("Mismatch in expected [%d] vs actual [%s] field count", fields.size(), row.size());
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN) {
      OperatorProfile profile = new OperatorProfile("compact-list-to-array");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      context.updateProfile(this, profile);
    }
    super.close(cascade);
  }
}
