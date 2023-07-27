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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Assigns each input file to its own split.
 *
 * Not named as a {@link JsonSubTypes} in {@link SplitHintSpec}, because this class is meant for internal use
 * within a server only. It is not serialized for transfer between servers, and is not part of the user-facing API.
 */
public class FilePerSplitHintSpec implements SplitHintSpec
{
  public static final FilePerSplitHintSpec INSTANCE = new FilePerSplitHintSpec();

  private FilePerSplitHintSpec()
  {
    // Singleton.
  }

  @Override
  public <T> Iterator<List<T>> split(
      final Iterator<T> inputIterator,
      final Function<T, InputFileAttribute> inputAttributeExtractor
  )
  {
    return Iterators.transform(inputIterator, Collections::singletonList);
  }
}
