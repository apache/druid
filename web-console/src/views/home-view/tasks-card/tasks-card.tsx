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
import React from 'react';

import { PluralPairIfNeeded } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { Capabilities, lookupBy, pluralIfNeeded, queryDruidSql } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

function getTaskStatus(d: any) {
  return d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode;
}

export interface TaskCounts {
  SUCCESS?: number;
  FAILED?: number;
  RUNNING?: number;
  PENDING?: number;
  WAITING?: number;
}

export interface TasksCardProps {
  capabilities: Capabilities;
}

export const TasksCard = React.memo(function TasksCard(props: TasksCardProps) {
  const [taskCountState] = useQueryManager<Capabilities, TaskCounts>({
    processQuery: async capabilities => {
      if (capabilities.hasSql()) {
        const taskCountsFromQuery: { status: string; count: number }[] = await queryDruidSql({
          query: `SELECT
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  COUNT (*) AS "count"
FROM sys.tasks
GROUP BY 1`,
        });
        return lookupBy(
          taskCountsFromQuery,
          x => x.status,
          x => x.count,
        );
      } else if (capabilities.hasOverlordAccess()) {
        const tasks: any[] = (await Api.instance.get('/druid/indexer/v1/tasks')).data;
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
    initQuery: props.capabilities,
  });

  const taskCounts = taskCountState.data;
  const successTaskCount = taskCounts ? taskCounts.SUCCESS : 0;
  const failedTaskCount = taskCounts ? taskCounts.FAILED : 0;
  const runningTaskCount = taskCounts ? taskCounts.RUNNING : 0;
  const pendingTaskCount = taskCounts ? taskCounts.PENDING : 0;
  const waitingTaskCount = taskCounts ? taskCounts.WAITING : 0;

  return (
    <HomeViewCard
      className="tasks-card"
      href="#ingestion"
      icon={IconNames.GANTT_CHART}
      title="Tasks"
      loading={taskCountState.loading}
      error={taskCountState.error}
    >
      <PluralPairIfNeeded
        firstCount={runningTaskCount}
        firstSingular="running task"
        secondCount={pendingTaskCount}
        secondSingular="pending task"
      />
      {Boolean(successTaskCount) && (
        <p>{pluralIfNeeded(successTaskCount || 0, 'successful task')}</p>
      )}
      {Boolean(waitingTaskCount) && <p>{pluralIfNeeded(waitingTaskCount || 0, 'waiting task')}</p>}
      {Boolean(failedTaskCount) && <p>{pluralIfNeeded(failedTaskCount || 0, 'failed task')}</p>}
      {!(
        Boolean(runningTaskCount) ||
        Boolean(pendingTaskCount) ||
        Boolean(successTaskCount) ||
        Boolean(waitingTaskCount) ||
        Boolean(failedTaskCount)
      ) && <p>There are no tasks</p>}
    </HomeViewCard>
  );
});
