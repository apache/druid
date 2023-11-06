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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.InputSourceFactory;
import org.apache.druid.data.input.impl.systemfield.SystemFields;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class LocalInputSourceFactory implements InputSourceFactory
{

  @Override
  public LocalInputSource create(List<String> inputFilePaths)
  {
    return new LocalInputSource(
        null,
        null,
        inputFilePaths.stream().map(chosenPath -> new File(chosenPath)).collect(
            Collectors.toList()),
        SystemFields.none()
    );
  }
}
