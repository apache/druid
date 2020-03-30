/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';

import { PluralPairIfNeeded } from '../../../components/plural-pair-if-needed/plural-pair-if-needed';
import { lookupBy, pluralIfNeeded, queryDruidSql, QueryManager } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { HomeViewCard } from '../home-view-card/home-view-card';

function getTaskStatus(d: any) {
  return d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode;
}

export interface TasksCardProps {
  capabilities: Capabilities;
}

export interface TasksCardState {
  taskCountLoading: boolean;
  runningTaskCount: number;
  pendingTaskCount: number;
  successTaskCount: number;
  failedTaskCount: number;
  waitingTaskCount: number;
  taskCountError?: string;
}

export class TasksCard extends React.PureComponent<TasksCardProps, TasksCardState> {
  private taskQueryManager: QueryManager<Capabilities, any>;

  constructor(props: TasksCardProps, context: any) {
    super(props, context);
    this.state = {
      taskCountLoading: false,
      runningTaskCount: 0,
      pendingTaskCount: 0,
      successTaskCount: 0,
      failedTaskCount: 0,
      waitingTaskCount: 0,
    };

    this.taskQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          const taskCountsFromQuery: { status: string; count: number }[] = await queryDruidSql({
            query: `SELECT
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  COUNT (*) AS "count"
FROM sys.tasks
GROUP BY 1`,
          });
          return lookupBy(taskCountsFromQuery, x => x.status, x => x.count);
        } else if (capabilities.hasOverlordAccess()) {
          const tasks: any[] = (await axios.get('/druid/indexer/v1/tasks')).data;
          return {
            SUCCESS: tasks.filter(d => getTaskStatus(d) === 'SUCCESS').length,
            FAILED: tasks.filter(d => getTaskStatus(d) === 'FAILED').length,
            RUNNING: tasks.filter(d => getTaskStatus(d) === 'RUNNING').length,
            PENDING: tasks.filter(d => getTaskStatus(d) === 'PENDING').length,
            WAITING: tasks.filter(d => getTaskStatus(d) === 'WAITING').length,
          };
        } else {
          throw new Error(`must have SQL or overlord access`);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          taskCountLoading: loading,
          successTaskCount: result ? result.SUCCESS : 0,
          failedTaskCount: result ? result.FAILED : 0,
          runningTaskCount: result ? result.RUNNING : 0,
          pendingTaskCount: result ? result.PENDING : 0,
          waitingTaskCount: result ? result.WAITING : 0,
          taskCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.taskQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.taskQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      taskCountError,
      taskCountLoading,
      runningTaskCount,
      pendingTaskCount,
      successTaskCount,
      failedTaskCount,
      waitingTaskCount,
    } = this.state;

    return (
      <HomeViewCard
        className="tasks-card"
        href={'#ingestion'}
        icon={IconNames.GANTT_CHART}
        title={'Tasks'}
        loading={taskCountLoading}
        error={taskCountError}
      >
        <PluralPairIfNeeded
          firstCount={runningTaskCount}
          firstSingular="running task"
          secondCount={pendingTaskCount}
          secondSingular="pending task"
        />
        {Boolean(successTaskCount) && <p>{pluralIfNeeded(successTaskCount, 'successful task')}</p>}
        {Boolean(waitingTaskCount) && <p>{pluralIfNeeded(waitingTaskCount, 'waiting task')}</p>}
        {Boolean(failedTaskCount) && <p>{pluralIfNeeded(failedTaskCount, 'failed task')}</p>}
        {!(
          Boolean(runningTaskCount) ||
          Boolean(pendingTaskCount) ||
          Boolean(successTaskCount) ||
          Boolean(waitingTaskCount) ||
          Boolean(failedTaskCount)
        ) && <p>There are no tasks</p>}
      </HomeViewCard>
    );
  }
}
