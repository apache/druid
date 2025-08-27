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

package org.apache.druid.msq.dart.controller.messages;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.msq.dart.worker.DartControllerClient;
import org.apache.druid.msq.exec.Controller;

/**
 * Messages sent from worker to controller by {@link DartControllerClient}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PartialKeyStatistics.class, name = "partialKeyStatistics"),
    @JsonSubTypes.Type(value = DoneReadingInput.class, name = "doneReadingInput"),
    @JsonSubTypes.Type(value = ResultsComplete.class, name = "resultsComplete"),
    @JsonSubTypes.Type(value = WorkerError.class, name = "workerError"),
    @JsonSubTypes.Type(value = WorkerWarning.class, name = "workerWarning")
})
public interface ControllerMessage
{
  /**
   * Query ID, to identify the controller that is being contacted.
   */
  String getQueryId();

  /**
   * Handler for this message, which calls an appropriate method on {@link Controller}.
   */
  void handle(Controller controller);
}
