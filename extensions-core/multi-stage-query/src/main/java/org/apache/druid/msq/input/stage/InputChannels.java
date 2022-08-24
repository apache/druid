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

package org.apache.druid.msq.input.stage;

import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.msq.kernel.StagePartition;

import java.io.IOException;

/**
 * Provides a way to open channels to read the outputs of prior stages. Used by {@link StageInputSliceReader}.
 */
public interface InputChannels
{
  /**
   * Open a channel to the given output partition of the given stage.
   */
  ReadableFrameChannel openChannel(StagePartition stagePartition) throws IOException;

  /**
   * Frame reader for output of the given stage.
   */
  FrameReader frameReader(int stageNumber);
}
