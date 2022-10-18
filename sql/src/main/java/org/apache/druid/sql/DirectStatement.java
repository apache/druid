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

package org.apache.druid.sql;

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Lifecycle for direct SQL statement execution, which means that the query
 * is planned and executed in a single step, with no "prepare" step.
 * Callers need call only:
 * <ul>
 * <li>{@link #execute()} to execute the query. The caller must close
 * the returned {@code Sequence}.</li>
 * <li>{@link #close()} to report metrics, or {@link #closeQuietly()}
 * otherwise.</li>
 * </ul>
 * <p>
 * The {@link #cancel()} method may be called from any thread and cancels
 * the query.
 * <p>
 * All other methods are optional and are generally for introspection.
 * <p>
 * The class supports two threading models. In the simple case, the same
 * thread creates this object and executes the query. In the split model,
 * a request thread creates this object and plans the query. A separate
 * response thread consumes results and performs any desired logging, etc.
 * The object is transferred between threads, with no overlapping access.
 * <p>
 * As statement holds no resources and need not be called. Only the
 * {@code Sequence} returned from {@link #execute()} need be closed.
 * <p>
 * Use this class for tests and JDBC execution. Use the HTTP variant,
 * {@link HttpStatement} for HTTP requests.
 */
public class DirectStatement extends AbstractStatement implements Cancelable
{
  private static final Logger log = new Logger(DirectStatement.class);

  /**
   * Represents the execution plan for a query with the ability to run
   * that plan (once).
   */
  public class ResultSet implements Cancelable
  {
    private final PlannerResult plannerResult;

    public ResultSet(PlannerResult plannerResult)
    {
      this.plannerResult = plannerResult;
    }

    public SqlQueryPlus query()
    {
      return queryPlus;
    }

    /**
     * Convenience method for the split plan/run case to ensure that the statement
     * can, in fact, be run.
     */
    public boolean runnable()
    {
      return plannerResult != null && plannerResult.runnable();
    }

    /**
     * Do the actual execute step which allows subclasses to wrap the sequence,
     * as is sometimes needed for testing.
     */
    public QueryResponse<Object[]> run()
    {
      try {
        // Check cancellation. Required for SqlResourceTest to work.
        transition(State.RAN);
        return plannerResult.run();
      }
      catch (RuntimeException e) {
        reporter.failed(e);
        throw e;
      }
    }

    public SqlRowTransformer createRowTransformer()
    {
      return new SqlRowTransformer(plannerContext.getTimeZone(), plannerResult.rowType());
    }

    public SqlExecutionReporter reporter()
    {
      return reporter;
    }

    @Override
    public Set<ResourceAction> resources()
    {
      return DirectStatement.this.resources();
    }

    @Override
    public void cancel()
    {
      DirectStatement.this.cancel();
    }

    public void close()
    {
      DirectStatement.this.close();
    }
  }

  private enum State
  {
    START,
    PREPARED,
    RAN,
    CANCELLED,
    FAILED,
    CLOSED
  }

  protected PrepareResult prepareResult;
  protected ResultSet resultSet;
  private volatile State state = State.START;

  public DirectStatement(
      final SqlToolbox lifecycleToolbox,
      final SqlQueryPlus queryPlus,
      final String remoteAddress
  )
  {
    super(lifecycleToolbox, queryPlus, remoteAddress);
  }

  public DirectStatement(
      final SqlToolbox lifecycleToolbox,
      final SqlQueryPlus sqlRequest
  )
  {
    super(lifecycleToolbox, sqlRequest, null);
  }

  /**
   * Convenience method to perform Direct execution of a query. Does both
   * the {@link #plan()} step and the {@link ResultSet#run()} step.
   *
   * @return sequence which delivers query results
   */
  public QueryResponse<Object[]> execute()
  {
    return plan().run();
  }

  /**
   * Prepares and plans a query for execution, returning a result set to
   * execute the query. In Druid, prepare and plan are different: prepare provides
   * information about the query, but plan does the "real" preparation to create
   * an actual executable plan.
   * <ul>
   * <li>Create the planner.</li>
   * <li>Parse the statement.</li>
   * <li>Provide parameters using a <a href="https://github.com/apache/druid/pull/6974">
   * "query optimized"</a> structure.</li>
   * <li>Validate the query against the Druid catalog.</li>
   * <li>Authorize access to the resources which the query needs.</li>
   * <li>Plan the query.</li>
   * </ul>
   * Call {@link ResultSet#run()} to run the resulting plan.
   */
  public ResultSet plan()
  {
    if (state != State.START) {
      throw new ISE("Can plan a query only once.");
    }
    long planningStartNanos = System.nanoTime();
    try (DruidPlanner planner = sqlToolbox.plannerFactory.createPlanner(
        sqlToolbox.engine,
        queryPlus.sql(),
        queryContext,
        // Context keys for authorization. Use the user-provided keys,
        // NOT the keys from the query context which, by this point,
        // will have been extended with internally-defined values.
        queryPlus.context().keySet())) {
      validate(planner);
      authorize(planner, authorizer());

      // Adding the statement to the lifecycle manager allows cancellation.
      // Tests cancel during this call; real clients might do so if the plan
      // or execution prep stages take too long for some unexpected reason.
      sqlToolbox.sqlLifecycleManager.add(sqlQueryId(), this);
      transition(State.PREPARED);
      resultSet = createResultSet(createPlan(planner));
      prepareResult = planner.prepareResult();
      // Double check needed by SqlResourceTest
      transition(State.PREPARED);
      reporter.planningTimeNanos(System.nanoTime() - planningStartNanos);
      return resultSet;
    }
    catch (RuntimeException e) {
      state = State.FAILED;
      reporter.failed(e);
      throw e;
    }
  }

  /**
   * Plan the query, which also produces the sequence that runs
   * the query.
   */
  @VisibleForTesting
  protected PlannerResult createPlan(DruidPlanner planner)
  {
    try {
      return planner.plan();
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
    }
  }

  /**
   * Wrapper around result set creation for the sole purpose of tests which
   * inject failures.
   */
  @VisibleForTesting
  protected ResultSet createResultSet(PlannerResult plannerResult)
  {
    return new ResultSet(plannerResult);
  }

  public PrepareResult prepareResult()
  {
    return prepareResult;
  }

  /**
   * Checks for cancellation. As it turns out, this is really just a test-time
   * check: an actual client can't cancel the query until the query reports
   * a query ID, which won't happen until after the {@link #execute())}
   * call.
   */
  private void transition(State newState)
  {
    if (state == State.CANCELLED) {
      throw new QueryInterruptedException(
          QueryInterruptedException.QUERY_CANCELED,
          StringUtils.format("Query is canceled [%s]", sqlQueryId()),
          null,
          null
      );
    }
    state = newState;
  }

  @Override
  public void cancel()
  {
    if (state == State.CLOSED) {
      return;
    }
    state = State.CANCELLED;
    final CopyOnWriteArrayList<String> nativeQueryIds = plannerContext.getNativeQueryIds();

    for (String nativeQueryId : nativeQueryIds) {
      log.debug("Canceling native query [%s]", nativeQueryId);
      sqlToolbox.queryScheduler.cancelQuery(nativeQueryId);
    }
  }

  @Override
  public void close()
  {
    if (state != State.START && state != State.CLOSED) {
      super.close();
      state = State.CLOSED;
    }
  }

  @Override
  public void closeWithError(Throwable e)
  {
    if (state != State.START && state != State.CLOSED) {
      super.closeWithError(e);
      state = State.CLOSED;
    }
  }
}
