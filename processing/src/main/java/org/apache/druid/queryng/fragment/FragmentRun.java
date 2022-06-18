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

package org.apache.druid.queryng.fragment;

import org.apache.druid.queryng.operators.Operator.ResultIterator;

import java.util.List;

/**
 * Runs the DAG. The fragment is opened (open is called on the root
 * operator) upon construction. Callers can then obtain the iterator,
 * or convert the DAG to a sequence or list. Callers <b>must</b> close
 * this object at the end of the run.
 *
 * Callers should generally use only one of the access methods: obtain
 * the iterator, a sequence, or convert the results to a list. The
 * fragment is not reentrant: results can be obtained only once.
 */
public interface FragmentRun<T> extends AutoCloseable
{
  FragmentContext context();

  ResultIterator<T> iterator();

  /**
   * Materializes the entire result set as a list. Primarily for testing.
   * Opens the fragment, reads results, and closes the fragment.
   */
  List<T> toList();

  @Override
  void close();
}
