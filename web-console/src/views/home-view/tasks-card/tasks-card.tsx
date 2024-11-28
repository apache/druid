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
import type { CancelToken } from 'axios';
import React from 'react';

import { PluralPairIfNeeded } from '../../../components';
import type { CapacityInfo } from '../../../druid-models';
import type { Capabilities } from '../../../helpers';
import { getClusterCapacity } from '../../../helpers';
import { useQueryManager } from '../../../hooks';
import { getApiArray, groupByAsMap, lookupBy, pluralIfNeeded, queryDruidSql } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

function getTaskStatus(d: any) {
  return d.statusCode === 'RUNNING' ? d.runnerStatusCode : d.statusCode;
}

export interface TaskCounts {
  success?: number;
  failed?: number;
  running?: number;
  pending?: number;
  waiting?: number;
}

async function getTaskCounts(
  capabilities: Capabilities,
  cancelToken: CancelToken,
): Promise<TaskCounts> {
  if (capabilities.hasSql()) {
    const taskCountsFromQuery = await queryDruidSql<{ status: string; count: number }>(
      {
        query: `SELECT
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  COUNT(*) AS "count"
FROM sys.tasks
GROUP BY 1`,
      },
      cancelToken,
    );
    return lookupBy(
      taskCountsFromQuery,
      x => x.status.toLowerCase(),
      x => x.count,
    );
  } else if (capabilities.hasOverlordAccess()) {
    const tasks: any[] = await getApiArray('/druid/indexer/v1/tasks', cancelToken);
    return groupByAsMap(
      tasks,
      d => getTaskStatus(d).toLowerCase(),
      xs => xs.length,
    );
  } else {
    throw new Error(`must have SQL or overlord access`);
  }
}

export interface TaskCountsAndCapacity extends TaskCounts, Partial<CapacityInfo> {}

export interface TasksCardProps {
  capabilities: Capabilities;
}

export const TasksCard = React.memo(function TasksCard(props: TasksCardProps) {
  const [cardState] = useQueryManager<Capabilities, TaskCountsAndCapacity>({
    initQuery: props.capabilities,
    processQuery: async (capabilities, cancelToken) => {
      const taskCounts = await getTaskCounts(capabilities, cancelToken);
      if (!capabilities.hasOverlordAccess()) return taskCounts;

      const capacity = await getClusterCapacity();
      return { ...taskCounts, ...capacity };
    },
  });

  const { success, failed, running, pending, waiting, totalTaskSlots } = cardState.data || {};

  return (
    <HomeViewCard
      className="tasks-card"
      href="#tasks"
      icon={IconNames.GANTT_CHART}
      title="Tasks"
      loading={cardState.loading}
      error={cardState.error}
    >
      {Boolean(totalTaskSlots) && <p>{pluralIfNeeded(totalTaskSlots || 0, 'task slot')}</p>}
      <PluralPairIfNeeded
        firstCount={running}
        firstSingular="running task"
        secondCount={pending}
        secondSingular="pending task"
      />
      {Boolean(success) && <p>{pluralIfNeeded(success || 0, 'successful task')}</p>}
      {Boolean(waiting) && <p>{pluralIfNeeded(waiting || 0, 'waiting task')}</p>}
      {Boolean(failed) && <p>{pluralIfNeeded(failed || 0, 'failed task')}</p>}
      {!(
        Boolean(running) ||
        Boolean(pending) ||
        Boolean(success) ||
        Boolean(waiting) ||
        Boolean(failed)
      ) && <p>No tasks</p>}
    </HomeViewCard>
  );
});
