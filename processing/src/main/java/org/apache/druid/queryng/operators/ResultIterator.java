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

package org.apache.druid.queryng.operators;

/**
 * Iterator over operator results. Operators do not use the Java
 * {@code Iterator} class: the simpler implementation here
 * minimizes per-row overhead. An {@code OperatorIterator} can
 * be converted to a Java {@code Iterator} by calling
 * {@link static <T> Iterator<T> Operators#toIterator(Operator<T>)},
 * but that approach adds overhead.
 */
public interface ResultIterator<T>
{
  /**
   * Result iterator that can be closed. Generally the operator handles
   * closing, but an iterator may hold resources that are to be closed.
   * The {@code cascade} parameter exists for the case in which the
   * iterator wraps a child operator: its meaning is the same as for
   * the operator itself.
   */
  interface CloseableResultIterator<T> extends ResultIterator<T>
  {
    void close(boolean cascade);
  }

  /**
   * Exception thrown at EOF.
   */
  class EofException extends Exception
  {
  }

  /**
   * Return the next row (item) of data. Throws {@link EofException}
   * at EOF. This structure avoids the need for a "has next" check,
   * streamlining tight inner loops.
   * @return
   * @throws ResultIterator.EofException
   */
  T next() throws EofException;
}
