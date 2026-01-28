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
import type { JSX } from 'react';
import { useState } from 'react';

import { NumericInputDialog } from '../../../dialogs';
import type { QueryContext } from '../../../druid-models';
import { getQueryContextKey } from '../../../druid-models';
import { getLink } from '../../../links';
import { capitalizeFirst, deleteKeys, formatInteger, tickIcon } from '../../../utils';

const DEFAULT_MAX_TASKS_OPTIONS = [2, 3, 4, 5, 7, 9, 11, 17, 33, 65, 129];
const TASK_DOCUMENTATION_LINK = `${getLink('DOCS')}/multi-stage-query/reference#context-parameters`;

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
  maxTasksOptions?: number[];
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
    maxTasksOptions = DEFAULT_MAX_TASKS_OPTIONS,
    ...rest
  } = props;
  const [customMaxNumTasksDialogOpen, setCustomMaxNumTasksDialogOpen] = useState(false);

  const maxNumTasks = queryContext.maxNumTasks;
  const taskAssigment = getQueryContextKey('taskAssignment', queryContext, defaultQueryContext);

  const fullClusterCapacity =
    typeof clusterCapacity === 'number' ? fullClusterCapacityLabelFn(clusterCapacity) : undefined;

  const shownMaxNumTaskOptions = clusterCapacity
    ? maxTasksOptions.filter(_ => _ <= clusterCapacity)
    : maxTasksOptions;

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
                shouldDismissPopover
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
                  shouldDismissPopover
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
              <MenuItem
                icon={tickIcon(taskAssigment === 'max')}
                text={
                  <>
                    <strong>Max</strong>: uses the maximum possible tasks up to the specified limit.
                  </>
                }
                multiline
                onClick={() => changeQueryContext({ ...queryContext, taskAssignment: 'max' })}
              />

              <MenuItem
                icon={tickIcon(taskAssigment === 'auto')}
                text={
                  <>
                    <strong>Auto</strong>: uses the minimum number of tasks while{' '}
                    <span
                      style={{
                        color: '#3eadf9',
                        cursor: 'pointer',
                      }}
                      onClick={e => {
                        window.open(TASK_DOCUMENTATION_LINK, '_blank');
                        e.stopPropagation();
                      }}
                    >
                      staying within constraints.
                    </span>
                  </>
                }
                multiline
                onClick={() => changeQueryContext({ ...queryContext, taskAssignment: 'auto' })}
              />
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
