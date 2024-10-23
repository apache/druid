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

package org.apache.druid.benchmark.query;

import org.apache.druid.java.util.common.StringUtils;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.List;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlGroupByBenchmark extends SqlBaseQueryBenchmark
{
  @Param({
      "string-Sequential-100_000",
      "string-Sequential-10_000_000",
      // "string-Sequential-1_000_000_000",
      "string-ZipF-1_000_000",
      "string-Uniform-1_000_000",

      "multi-string-Sequential-100_000",
      "multi-string-Sequential-10_000_000",
      // "multi-string-Sequential-1_000_000_000",
      "multi-string-ZipF-1_000_000",
      "multi-string-Uniform-1_000_000",

      "long-Sequential-100_000",
      "long-Sequential-10_000_000",
      // "long-Sequential-1_000_000_000",
      "long-ZipF-1_000_000",
      "long-Uniform-1_000_000",

      "double-ZipF-1_000_000",
      "double-Uniform-1_000_000",

      "float-ZipF-1_000_000",
      "float-Uniform-1_000_000",

      "stringArray-Sequential-100_000",
      "stringArray-Sequential-3_000_000",
      // "stringArray-Sequential-1_000_000_000",
      "stringArray-ZipF-1_000_000",
      "stringArray-Uniform-1_000_000",

      "longArray-Sequential-100_000",
      "longArray-Sequential-3_000_000",
      // "longArray-Sequential-1_000_000_000",
      "longArray-ZipF-1_000_000",
      "longArray-Uniform-1_000_000",

      "nested-Sequential-100_000",
      "nested-Sequential-3_000_000",
      // "nested-Sequential-1_000_000_000",
      "nested-ZipF-1_000_000",
      "nested-Uniform-1_000_000",
  })
  private String groupingDimension;


  @Override
  public String getQuery()
  {
    return StringUtils.format("SELECT \"%s\", COUNT(*) FROM druid.%s GROUP BY 1", groupingDimension, SqlBenchmarkDatasets.GROUPER);
  }

  @Override
  public List<String> getDatasources()
  {
    return Collections.singletonList(SqlBenchmarkDatasets.GROUPER);
  }
}
