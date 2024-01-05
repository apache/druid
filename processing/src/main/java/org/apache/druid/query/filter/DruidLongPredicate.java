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

package org.apache.druid.query.filter;

/**
 * Note: this is not a {@link org.apache.druid.guice.annotations.PublicApi} or an
 * {@link org.apache.druid.guice.annotations.ExtensionPoint} of Druid.
 */
public interface DruidLongPredicate
{
  DruidLongPredicate ALWAYS_FALSE_WITH_NULL_UNKNOWN = input -> DruidPredicateMatch.FALSE;

  DruidLongPredicate ALWAYS_TRUE = input -> DruidPredicateMatch.TRUE;

  DruidLongPredicate MATCH_NULL_ONLY = new DruidLongPredicate()
  {
    @Override
    public DruidPredicateMatch applyLong(long input)
    {
      return DruidPredicateMatch.FALSE;
    }

    @Override
    public DruidPredicateMatch applyNull()
    {
      return DruidPredicateMatch.TRUE;
    }
  };

  DruidPredicateMatch applyLong(long input);

  default DruidPredicateMatch applyNull()
  {
    return DruidPredicateMatch.UNKNOWN;
  }
}
