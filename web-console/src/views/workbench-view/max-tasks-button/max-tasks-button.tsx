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

import type { ButtonProps } from '@blueprintjs/core';
import { Button, Menu, MenuDivider, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { JSX, ReactNode } from 'react';
import React, { useState } from 'react';

import { NumericInputDialog } from '../../../dialogs';
import type { QueryContext, TaskAssignment } from '../../../druid-models';
import { getQueryContextKey } from '../../../druid-models';
import { getLink } from '../../../links';
import { capitalizeFirst, deleteKeys, formatInteger, tickIcon } from '../../../utils';

const MAX_NUM_TASK_OPTIONS = [2, 3, 4, 5, 7, 9, 11, 17, 33, 65, 129];
const TASK_ASSIGNMENT_OPTIONS: TaskAssignment[] = ['max', 'auto'];

const TASK_ASSIGNMENT_DESCRIPTION: Record<string, string> = {
  max: 'uses the maximum possible tasks up to the specified limit.',
  auto: 'maximizes the number of tasks while staying within 512 MiB or 10,000 files per task, unless more tasks are needed to stay under the max task limit.',
};

const TASK_ASSIGNMENT_LABEL_ELEMENT: Record<string, ReactNode> = {
  auto: (
    <Button
      icon={IconNames.HELP}
      minimal
      onClick={() =>
        window.open(`${getLink('DOCS')}/multi-stage-query/reference#context-parameters`, '_blank')
      }
    />
  ),
};

const DEFAULT_MAX_NUM_TASKS_LABEL_FN = (maxNum: number) => {
  if (maxNum === 2) return { text: formatInteger(maxNum), label: '(1 controller + 1 worker)' };
  return { text: formatInteger(maxNum), label: `(1 controller + max ${maxNum - 1} workers)` };
};

const DEFAULT_FULL_CLUSTER_CAPACITY_LABEL_FN = (clusterCapacity: number) =>
  `${formatInteger(clusterCapacity)} (full cluster capacity)`;

export interface MaxTasksButtonProps extends Omit<ButtonProps, 'text' | 'rightIcon'> {
  clusterCapacity: number | undefined;
  queryContext: QueryContext;
  changeQueryContext(queryContext: QueryContext): void;
  defaultQueryContext: QueryContext;
  menuHeader?: JSX.Element;
  maxTasksLabelFn?: (maxNum: number) => { text: string; label?: string };
  fullClusterCapacityLabelFn?: (clusterCapacity: number) => string;
}

export const MaxTasksButton = function MaxTasksButton(props: MaxTasksButtonProps) {
  const {
    clusterCapacity,
    queryContext,
    changeQueryContext,
    defaultQueryContext,
    menuHeader,
    maxTasksLabelFn = DEFAULT_MAX_NUM_TASKS_LABEL_FN,
    fullClusterCapacityLabelFn = DEFAULT_FULL_CLUSTER_CAPACITY_LABEL_FN,
    ...rest
  } = props;
  const [customMaxNumTasksDialogOpen, setCustomMaxNumTasksDialogOpen] = useState(false);

  const maxNumTasks = queryContext.maxNumTasks;
  const taskAssigment = getQueryContextKey('taskAssignment', queryContext, defaultQueryContext);

  const fullClusterCapacity =
    typeof clusterCapacity === 'number' ? fullClusterCapacityLabelFn(clusterCapacity) : undefined;

  const shownMaxNumTaskOptions = clusterCapacity
    ? MAX_NUM_TASK_OPTIONS.filter(_ => _ <= clusterCapacity)
    : MAX_NUM_TASK_OPTIONS;

  return (
    <>
      <Popover
        className="max-tasks-button"
        position={Position.BOTTOM_LEFT}
        content={
          <Menu>
            {menuHeader}
            <MenuDivider title="Maximum number of tasks to launch" />
            {Boolean(fullClusterCapacity) && (
              <MenuItem
                icon={tickIcon(typeof maxNumTasks === 'undefined')}
                text={fullClusterCapacity}
                onClick={() => changeQueryContext(deleteKeys(queryContext, ['maxNumTasks']))}
              />
            )}
            {shownMaxNumTaskOptions.map(m => {
              const { text, label } = maxTasksLabelFn(m);

              return (
                <MenuItem
                  key={String(m)}
                  icon={tickIcon(m === maxNumTasks)}
                  text={text}
                  label={label}
                  onClick={() => changeQueryContext({ ...queryContext, maxNumTasks: m })}
                />
              );
            })}
            <MenuItem
              icon={tickIcon(
                typeof maxNumTasks === 'number' && !shownMaxNumTaskOptions.includes(maxNumTasks),
              )}
              text="Custom"
              onClick={() => setCustomMaxNumTasksDialogOpen(true)}
            />
            <MenuDivider />
            <MenuItem
              icon={IconNames.FLOW_BRANCH}
              text="Task assignment"
              label={capitalizeFirst(taskAssigment)}
              submenuProps={{ style: { width: 300 } }}
            >
              {TASK_ASSIGNMENT_OPTIONS.map(t => (
                <MenuItem
                  key={String(t)}
                  icon={tickIcon(t === taskAssigment)}
                  text={
                    <>
                      <strong>{capitalizeFirst(t)}</strong>: {TASK_ASSIGNMENT_DESCRIPTION[t]}
                    </>
                  }
                  labelElement={TASK_ASSIGNMENT_LABEL_ELEMENT[t]}
                  shouldDismissPopover={false}
                  multiline
                  onClick={() => changeQueryContext({ ...queryContext, taskAssignment: t })}
                />
              ))}
            </MenuItem>
          </Menu>
        }
      >
        <Button
          {...rest}
          text={`Max tasks: ${
            typeof maxNumTasks === 'undefined'
              ? clusterCapacity
                ? fullClusterCapacity
                : 2
              : formatInteger(maxNumTasks)
          }`}
          rightIcon={IconNames.CARET_DOWN}
        />
      </Popover>
      {customMaxNumTasksDialogOpen && (
        <NumericInputDialog
          title="Custom max task number"
          message={
            <>
              <p>Specify the total number of tasks that should be launched.</p>
              <p>
                There must be at least 2 tasks to account for the controller and at least one
                worker.
              </p>
            </>
          }
          minValue={2}
          integer
          initValue={maxNumTasks || 2}
          onSubmit={maxNumTasks => {
            changeQueryContext({ ...queryContext, maxNumTasks });
          }}
          onClose={() => setCustomMaxNumTasksDialogOpen(false)}
        />
      )}
    </>
  );
};
