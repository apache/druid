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

import { Button, ButtonProps, Menu, MenuDivider, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import { NumericInputDialog } from '../../../dialogs';
import {
  changeMaxNumTasks,
  changeTaskAssigment,
  getMaxNumTasks,
  getTaskAssigment,
  QueryContext,
} from '../../../druid-models';
import { formatInteger, tickIcon } from '../../../utils';

const MAX_NUM_TASK_OPTIONS = [2, 3, 4, 5, 7, 9, 11, 17, 33, 65];
const TASK_ASSIGNMENT_OPTIONS = ['max', 'auto'];

const TASK_ASSIGNMENT_DESCRIPTION: Record<string, string> = {
  max: 'Use as many tasks as possible, up to the maximum.',
  auto: 'Use as few tasks as possible without exceeding 10 GiB or 10,000 files per task.',
};

export interface MaxTasksButtonProps extends Omit<ButtonProps, 'text' | 'rightIcon'> {
  queryContext: QueryContext;
  changeQueryContext(queryContext: QueryContext): void;
}

export const MaxTasksButton = function MaxTasksButton(props: MaxTasksButtonProps) {
  const { queryContext, changeQueryContext, ...rest } = props;
  const [customMaxNumTasksDialogOpen, setCustomMaxNumTasksDialogOpen] = useState(false);

  const maxNumTasks = getMaxNumTasks(queryContext);
  const taskAssigment = getTaskAssigment(queryContext);

  return (
    <>
      <Popover2
        className="max-tasks-button"
        position={Position.BOTTOM_LEFT}
        content={
          <Menu>
            <MenuDivider title="Number of tasks to launch" />
            {MAX_NUM_TASK_OPTIONS.map(m => (
              <MenuItem
                key={String(m)}
                icon={tickIcon(m === maxNumTasks)}
                text={formatInteger(m)}
                label={`(1 controller + ${m === 2 ? '1 worker' : `max ${m - 1} workers`})`}
                onClick={() => changeQueryContext(changeMaxNumTasks(queryContext, m))}
              />
            ))}
            <MenuItem
              icon={tickIcon(!MAX_NUM_TASK_OPTIONS.includes(maxNumTasks))}
              text="Custom"
              onClick={() => setCustomMaxNumTasksDialogOpen(true)}
            />
            <MenuDivider />
            <MenuItem icon={IconNames.FLOW_BRANCH} text="Task assignment" label={taskAssigment}>
              {TASK_ASSIGNMENT_OPTIONS.map(t => (
                <MenuItem
                  key={String(t)}
                  icon={tickIcon(t === taskAssigment)}
                  text={`${t} - ${TASK_ASSIGNMENT_DESCRIPTION[t]}`}
                  shouldDismissPopover={false}
                  onClick={() => changeQueryContext(changeTaskAssigment(queryContext, t))}
                />
              ))}
            </MenuItem>
          </Menu>
        }
      >
        <Button {...rest} text={`Max tasks: ${maxNumTasks}`} rightIcon={IconNames.CARET_DOWN} />
      </Popover2>
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
          initValue={maxNumTasks}
          onSubmit={p => {
            changeQueryContext(changeMaxNumTasks(queryContext, p));
          }}
          onClose={() => setCustomMaxNumTasksDialogOpen(false)}
        />
      )}
    </>
  );
};
