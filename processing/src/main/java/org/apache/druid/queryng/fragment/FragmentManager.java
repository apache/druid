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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.queryng.operators.Temporary;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class FragmentManager implements FragmentContext, Closeable
{
  public static class OperatorChild
  {
    public final int position;
    public final Operator<?> operator;
    public final int sliceID;

    public OperatorChild(int position, Operator<?> operator, int sliceID)
    {
      super();
      this.position = position;
      this.operator = operator;
      this.sliceID = sliceID;
    }

    @Override
    public String toString()
    {
      String str = "{" + position + ": ";
      if (operator == null) {
        return str + "slice " + sliceID + "}";
      } else {
        return str + "operator " + operator.getClass().getSimpleName() + "}";
      }
    }
  }

  public static class OperatorTracker
  {
    public final Operator<?> operator;
    @SuppressWarnings("unused") // Needed in future steps
    public final int operatorId;
    public final List<OperatorChild> children = new ArrayList<>();
    public OperatorProfile profile;

    private OperatorTracker(Operator<?> op, int id)
    {
      this.operator = op;
      this.operatorId = id;
    }

    @Override
    public String toString()
    {
      String str = "OperatorTracker{"
          + "operator=" + operator.getClass().getSimpleName();
      if (!children.isEmpty()) {
        str += ", children=" + children;
      }
      return str + "}";
    }
  }

  private final QueryManager query;
  private final Map<Operator<?>, OperatorTracker> operators = new IdentityHashMap<>();
  private final ResponseContext responseContext;
  private final String queryId;
  private long startTimeMillis;
  private long closeTimeMillis;
  private long timeoutMs = Long.MAX_VALUE;
  private long timeoutAt;
  private final List<Exception> exceptions = new ArrayList<>();
  private final List<Consumer<FragmentManager>> closeListeners = new ArrayList<>();
  private State state = State.START;
  private Operator<?> rootOperator;
  private Sequence<?> rootSequence;

  public FragmentManager(
      final QueryManager query,
      final String queryId,
      final ResponseContext responseContext
  )
  {
    this.query = query;
    this.queryId = queryId;
    this.responseContext = responseContext;
    this.startTimeMillis = System.currentTimeMillis();
  }

  public QueryManager query()
  {
    return query;
  }

  @Override
  public State state()
  {
    return state;
  }

  @Override
  public String queryId()
  {
    return queryId;
  }

  @Override
  public ResponseContext responseContext()
  {
    return responseContext;
  }

  @SuppressWarnings("unused") // Needed in future steps
  public void setTimeout(long timeoutMs)
  {
    this.timeoutMs = timeoutMs;
    if (timeoutMs > 0) {
      this.timeoutAt = startTimeMillis + timeoutMs;
    } else {
      this.timeoutAt = JodaUtils.MAX_INSTANT;
    }
  }

  public void registerRoot(Operator<?> op)
  {
    rootOperator = op;
    rootSequence = null;
  }

  @Temporary
  public void registerRoot(Sequence<?> sequence)
  {
    rootSequence = sequence;
    rootOperator = null;
  }

  @Override
  public synchronized void register(Operator<?> op)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    OperatorTracker tracker = new OperatorTracker(op, operators.size() + 1);
    operators.put(op, tracker);
  }

  @Override
  public synchronized void registerChild(Operator<?> parent, Operator<?> child)
  {
    registerChild(parent, 0, child);
  }

  @Override
  public synchronized void registerChild(Operator<?> parent, int posn, Operator<?> child)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    OperatorTracker tracker = operators.get(parent);
    Preconditions.checkNotNull(tracker);
    tracker.children.add(new OperatorChild(posn, child, 0));
  }

  @Override
  public synchronized void registerChild(Operator<?> parent, int posn, int sliceID)
  {
    Preconditions.checkState(state == State.START || state == State.RUN);
    OperatorTracker tracker = operators.get(parent);
    Preconditions.checkNotNull(tracker);
    tracker.children.add(new OperatorChild(posn, null, sliceID));
  }

  /**
   * Reports the exception, if any, that terminated the fragment.
   * Should be non-null only if the state is {@code FAILED}.
   */
  public Exception exception()
  {
    if (exceptions.isEmpty()) {
      return null;
    } else {
      return exceptions.get(0);
    }
  }

  @Override
  public boolean failed()
  {
    return !exceptions.isEmpty();
  }

  @Temporary
  public boolean rootIsOperator()
  {
    return rootOperator != null;
  }

  @Temporary
  public boolean rootIsSequence()
  {
    return rootSequence != null;
  }

  /**
   * Return the root, if it exists, as an operator. If the root is already an
   * operator, return it. If it is a sequence either unwrap it to get an
   * operator, or wrap it to get an operator. Returns {@code null} if there
   * is no root at all.
   */
  @SuppressWarnings("unchecked")
  public <T> Operator<T> rootOperator()
  {
    if (rootOperator != null) {
      return (Operator<T>) rootOperator;
    } else if (rootSequence != null) {
      rootOperator = Operators.unwrapOperator((Sequence<T>) rootSequence);
      rootSequence = null;
      return (Operator<T>) rootOperator;
    } else {
      return null;
    }
  }

  /**
   * Return the root, if it exists, as a sequence. If the root is already a
   * sequence, return it. If it is an operator, wrap it in a sequence and
   * return it. Note the asymmetry with {@link #rootOperator}: there is no
   * attempt to unwrap a sequence behind an operator because this case should
   * never occur.
   */
  @Temporary
  @SuppressWarnings("unchecked")
  public <T> Sequence<T> rootSequence()
  {
    if (rootSequence != null) {
      return (Sequence<T>) rootSequence;
    } else if (rootOperator != null) {
      rootSequence = Operators.toSequence(rootOperator);
      rootOperator = null;
      return (Sequence<T>) rootSequence;
    } else {
      return null;
    }
  }

  /**
   * Run the root operator (converted from a sequence if necessary) and return
   * the operator's {@link ResultSequence}. If there is no root (pathological case),
   * create one as a null operator.
   */
  public <T> ResultIterator<T> run()
  {
    Preconditions.checkState(state == State.START);
    if (!rootIsOperator() && !rootIsSequence()) {
      registerRoot(new NullOperator<T>(this));
    }
    Operator<T> root = rootOperator();
    state = State.RUN;
    return root.open();
  }

  /**
   * Run the operator as a sequence. If there is no root, define one as an
   * empty sequence. If the root is an operator, wrap it in a sequence. Then
   * wrap the root sequence with baggage which will close the fragment.
   * @param <T>
   * @return
   */
  @SuppressWarnings("unchecked")
  @Temporary
  public <T> Sequence<T> runAsSequence()
  {
    Preconditions.checkState(state == State.START);
    if (!rootIsOperator() && !rootIsSequence()) {
      registerRoot(Sequences.empty());
    }
    state = State.RUN;
    return Sequences.withBaggage((Sequence<T>) rootSequence(), this);
  }

  /**
   * Materializes the entire result set as a list. Primarily for testing.
   * Opens the fragment, reads results, and closes the fragment.
   */
  public <T> List<T> toList()
  {
    try {
      if (rootOperator != null) {
        return Iterators.toList(run());
      } else if (rootSequence != null) {
        Sequence<T> seq = runAsSequence();
        return seq.toList();
      } else {
        return Collections.emptyList();
      }
    }
    catch (RuntimeException e) {
      failed(e);
      throw e;
    }
    finally {
      close();
    }
  }

  public void failed(Exception exception)
  {
    this.exceptions.add(exception);
    this.state = State.FAILED;
  }

  @Override
  public void checkTimeout()
  {
    if (timeoutAt > 0 && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(
          StringUtils.nonStrictFormat("Query [%s] timed out after [%d] ms",
              queryId, timeoutMs));
    }
  }

  @Override
  public void missingSegment(SegmentDescriptor descriptor)
  {
    responseContext.add(
        ResponseContext.Keys.MISSING_SEGMENTS,
        Collections.singletonList(descriptor)
    );
  }

  /**
   * Closes all operators from the leaves to the root.
   * As a result, operators must not call their children during
   * the {@code close()} call. Errors are collected, but all operators are closed
   * regardless of exceptions.
   */
  @Override
  public void close()
  {
    if (state == State.START) {
      state = State.CLOSED;
    }
    if (state == State.CLOSED) {
      return;
    }
    for (Operator<?> op : operators.keySet()) {
      try {
        op.close(false);
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
    state = State.CLOSED;
    closeTimeMillis = System.currentTimeMillis();
    for (Consumer<FragmentManager> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public synchronized void updateProfile(Operator<?> op, OperatorProfile profile)
  {
    OperatorTracker tracker = operators.get(op);
    Preconditions.checkNotNull(tracker);
    tracker.profile = profile;
  }

  public void onClose(Consumer<FragmentManager> listener)
  {
    closeListeners.add(listener);
  }

  public long elapsedTimeMs()
  {
    return closeTimeMillis - startTimeMillis;
  }

  protected Map<Operator<?>, OperatorTracker> operators()
  {
    return operators;
  }
}
