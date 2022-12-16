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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Utility class encapsulating a parse exception report used in tests.
 */
public class ParseExceptionReport
{
  private final List<String> inputs;
  private final List<String> errorMessages;

  private ParseExceptionReport(List<String> inputs, List<String> errorMessages)
  {
    this.inputs = inputs;
    this.errorMessages = errorMessages;
  }

  @SuppressWarnings("unchecked")
  public static ParseExceptionReport forPhase(
      IngestionStatsAndErrorsTaskReportData reportData,
      String phase
  )
  {
    List<LinkedHashMap<String, Object>> events =
        (List<LinkedHashMap<String, Object>>) reportData.getUnparseableEvents().get(phase);

    final List<String> inputs = new ArrayList<>();
    final List<String> errorMessages = new ArrayList<>();
    events.forEach(event -> {
      inputs.add((String) event.get("input"));
      errorMessages.add(((List<String>) event.get("details")).get(0));
    });

    return new ParseExceptionReport(inputs, errorMessages);
  }

  public List<String> getInputs()
  {
    return inputs;
  }

  public List<String> getErrorMessages()
  {
    return errorMessages;
  }

}
