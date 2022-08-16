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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;

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

  protected PrepareResult prepareResult;
  protected PlannerResult plannerResult;
  private volatile boolean canceled;

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
   * Direct execution of a query, including:
   * <ul>
   * <li>Create the planner.</li>
   * <li>Parse the statement.</li>
   * <li>Provide parameters using a <a href="https://github.com/apache/druid/pull/6974">
   * "query optimized"</a> structure.</li>
   * <li>Validate the query against the Druid catalog.</li>
   * <li>Authorize access to the resources which the query needs.</li>
   * <li>Plan the query.</li>
   * <li>Return a {@link Sequence} which executes the query and returns results.</li>
   * </ul>
   *
   * This method is called from the request thread; results are read in the
   * response thread.
   *
   * @return sequence which delivers query results
   */
  public Sequence<Object[]> execute()
  {
    try (DruidPlanner planner = sqlToolbox.plannerFactory.createPlanner(
        sqlToolbox.engine,
        queryPlus.sql(),
        queryPlus.context())) {
      validate(planner);
      authorize(planner, authorizer());

      // Adding the statement to the lifecycle manager allows cancellation.
      // Tests cancel during this call; real clients might do so if the plan
      // or execution prep stages take too long for some unexpected reason.
      sqlToolbox.sqlLifecycleManager.add(sqlQueryId(), this);
      checkCanceled();
      plannerResult = plan(planner);
      prepareResult = planner.prepareResult();
      return doExecute();
    }
    catch (RuntimeException e) {
      reporter.failed(e);
      throw e;
    }
  }

  public PrepareResult prepareResult()
  {
    return prepareResult;
  }

  /**
   * Do the actual execute step which allows subclasses to wrap the sequence,
   * as is sometimes needed for testing.
   */
  protected Sequence<Object[]> doExecute()
  {
    // Check cancellation here and not in execute() above:
    // required for SqlResourceTest to work.
    checkCanceled();
    try {
      return plannerResult.run();
    }
    catch (RuntimeException e) {
      reporter.failed(e);
      throw e;
    }
  }

  /**
   * Checks for cancellation. As it turns out, this is really just a test-time
   * check: an actual client can't cancel the query until the query reports
   * a query ID, which won't happen until after the {@link #execute())}
   * call.
   */
  private void checkCanceled()
  {
    if (canceled) {
      throw new QueryInterruptedException(
          QueryInterruptedException.QUERY_CANCELED,
          StringUtils.format("Query is canceled [%s]", sqlQueryId()),
          null,
          null
      );
    }
  }

  @Override
  public void cancel()
  {
    canceled = true;
    final CopyOnWriteArrayList<String> nativeQueryIds = plannerContext.getNativeQueryIds();

    for (String nativeQueryId : nativeQueryIds) {
      log.debug("Canceling native query [%s]", nativeQueryId);
      sqlToolbox.queryScheduler.cancelQuery(nativeQueryId);
    }
  }
}
