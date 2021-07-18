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

package org.apache.druid.segment.incremental.oak;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * This test class is a hack to initialize IncrementalIndexCreator to include OakIncrementalIndexSpec.
 * It is needed because all @Parameterized.Parameters methods are called before any other tests' code segments.
 * Even before @BeforeClass annotated methods.
 */
@RunWith(Parameterized.class)
public class OakDummyInitTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    // Add Oak to the available incremental indexes
    IncrementalIndexCreator.addIndexSpec(OakIncrementalIndexSpec.class, "oak");
    return ImmutableList.of();
  }
}
