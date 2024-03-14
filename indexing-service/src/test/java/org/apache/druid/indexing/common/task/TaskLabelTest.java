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


import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TaskLabelTest
{
  @Test
  public void testGetLabelsWhenLabelsExist()
  {
    List<String> expectedLabels = Arrays.asList("label1", "label2", "label3");

    TaskLabel taskLabel = new TaskLabel(expectedLabels);

    assertEquals(expectedLabels, taskLabel.getLabels());
  }

  @Test
  public void testGetLabelsWhenListIsEmpty()
  {
    List<String> emptyLabels = Collections.emptyList();

    TaskLabel taskLabel = new TaskLabel(emptyLabels);

    assertEquals(emptyLabels, taskLabel.getLabels());
  }

  @Test(expected = NullPointerException.class)
  public void testGetLabelsWhenListIsNull()
  {
    new TaskLabel(null);
  }
}
