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

package org.apache.druid.compressedbigdecimal;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;

public class CompressedBigDecimalMinSqlAggregatorTest extends CompressedBigDecimalSqlAggregatorTestBase
{
  private static final String FUNCTION_NAME = CompressedBigDecimalMinSqlAggregator.NAME;

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(ImmutableSet.of(new CompressedBigDecimalMinSqlAggregator()), ImmutableSet.of());
  }

  @Override
  public void testCompressedBigDecimalAggWithNumberParse()
  {
    testCompressedBigDecimalAggWithNumberParseHelper(
        FUNCTION_NAME,
        new Object[]{"1.000000000", "1.000000000", "0"},
        CompressedBigDecimalMinAggregatorFactory::new
    );
  }

  @Override
  public void testCompressedBigDecimalAggWithStrictNumberParse()
  {
    testCompressedBigDecimalAggWithStrictNumberParseHelper(
        FUNCTION_NAME,
        CompressedBigDecimalMinAggregatorFactory::new
    );
  }

  @Override
  public void testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScale()
  {
    testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScaleHelper(
        FUNCTION_NAME,
        new Object[]{"1.000", "1.000", "0"},
        CompressedBigDecimalMinAggregatorFactory::new
    );
  }

  @Override
  public void testCompressedBigDecimalAggDefaultScale()
  {
    testCompressedBigDecimalAggDefaultScaleHelper(
        FUNCTION_NAME,
        new Object[]{"1.000000000", "1.000000000", "0"},
        CompressedBigDecimalMinAggregatorFactory::new
    );
  }

  @Override
  public void testCompressedBigDecimalAggDefaultSizeAndScale()
  {
    testCompressedBigDecimalAggDefaultSizeAndScaleHelper(
        FUNCTION_NAME,
        new Object[]{"1.000000000", "1.000000000", "0"},
        CompressedBigDecimalMinAggregatorFactory::new
    );
  }
}
