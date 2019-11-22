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

import { Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { BasicAction, basicActionsToMenu } from '../../utils/basic-action';
import { ActionIcon } from '../action-icon/action-icon';

import './action-cell.scss';

export const ACTION_COLUMN_ID = 'actions';
export const ACTION_COLUMN_LABEL = 'Actions';
export const ACTION_COLUMN_WIDTH = 70;

export interface ActionCellProps {
  onDetail?: () => void;
  actions?: BasicAction[];
}

export const ActionCell = React.memo(function ActionCell(props: ActionCellProps) {
  const { onDetail, actions } = props;
  const actionsMenu = actions ? basicActionsToMenu(actions) : null;

  return (
    <div className="action-cell">
      {onDetail && <ActionIcon icon={IconNames.SEARCH_TEMPLATE} onClick={onDetail} />}
      {actionsMenu && (
        <Popover content={actionsMenu} position={Position.BOTTOM_RIGHT}>
          <ActionIcon icon={IconNames.WRENCH} />
        </Popover>
      )}
    </div>
  );
});
