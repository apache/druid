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

import {
  Button,
  ButtonGroup,
  Hotkey,
  Hotkeys,
  HotkeysTarget,
  Menu,
  MenuItem,
  Popover,
  Position,
  Tooltip,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { MenuCheckbox } from '../../../components';
import {
  getUseApproximateCountDistinct,
  getUseApproximateTopN,
  getUseCache,
  QueryContext,
  setUseApproximateCountDistinct,
  setUseApproximateTopN,
  setUseCache,
} from '../../../utils/query-context';
import { DRUID_DOCS_RUNE, DRUID_DOCS_SQL } from '../../../variables';

export interface RunButtonProps {
  runeMode: boolean;
  autoRun: boolean;
  setAutoRun: (autoRun: boolean) => void;
  wrapQuery: boolean;
  setWrapQuery: (wrapQuery: boolean) => void;
  queryContext: QueryContext;
  onQueryContextChange: (newQueryContext: QueryContext) => void;
  onRun: (() => void) | undefined;
  onExplain: (() => void) | undefined;
  onEditContext: () => void;
  onHistory: () => void;
}

@HotkeysTarget
export class RunButton extends React.PureComponent<RunButtonProps> {
  constructor(props: RunButtonProps, context: any) {
    super(props, context);
  }

  public renderHotkeys() {
    return (
      <Hotkeys>
        <Hotkey
          allowInInput
          global
          combo="ctrl + enter"
          label="run on click"
          onKeyDown={this.handleRun}
        />
      </Hotkeys>
    );
  }

  private handleRun = () => {
    const { onRun } = this.props;
    if (!onRun) return;
    onRun();
  };

  renderExtraMenu() {
    const {
      runeMode,
      onExplain,
      queryContext,
      onQueryContextChange,
      onEditContext,
      onHistory,
      autoRun,
      setAutoRun,
      wrapQuery,
      setWrapQuery,
    } = this.props;

    const useCache = getUseCache(queryContext);
    const useApproximateCountDistinct = getUseApproximateCountDistinct(queryContext);
    const useApproximateTopN = getUseApproximateTopN(queryContext);

    return (
      <Menu>
        <MenuItem
          icon={IconNames.HELP}
          text="Query docs"
          href={runeMode ? DRUID_DOCS_RUNE : DRUID_DOCS_SQL}
          target="_blank"
        />
        {!runeMode && (
          <>
            {onExplain && <MenuItem icon={IconNames.CLEAN} text="Explain" onClick={onExplain} />}
            <MenuItem icon={IconNames.HISTORY} text="History" onClick={onHistory} />
            <MenuCheckbox
              checked={wrapQuery}
              label="Wrap query with limit"
              onChange={() => setWrapQuery(!wrapQuery)}
            />
            <MenuCheckbox
              checked={autoRun}
              label="Auto run queries"
              onChange={() => setAutoRun(!autoRun)}
            />
            <MenuCheckbox
              checked={useApproximateCountDistinct}
              label="Use approximate COUNT(DISTINCT)"
              onChange={() => {
                onQueryContextChange(
                  setUseApproximateCountDistinct(queryContext, !useApproximateCountDistinct),
                );
              }}
            />
            <MenuCheckbox
              checked={useApproximateTopN}
              label="Use approximate TopN"
              onChange={() => {
                onQueryContextChange(setUseApproximateTopN(queryContext, !useApproximateTopN));
              }}
            />
          </>
        )}
        <MenuCheckbox
          checked={useCache}
          label="Use cache"
          onChange={() => {
            onQueryContextChange(setUseCache(queryContext, !useCache));
          }}
        />
        {!runeMode && (
          <MenuItem icon={IconNames.PROPERTIES} text="Edit context" onClick={onEditContext} />
        )}
      </Menu>
    );
  }

  render(): JSX.Element {
    const { runeMode, onRun, wrapQuery } = this.props;
    const runButtonText = runeMode ? 'Rune' : wrapQuery ? 'Run with limit' : 'Run as is';

    return (
      <ButtonGroup className="run-button">
        {onRun ? (
          <Tooltip content="Control + Enter" hoverOpenDelay={900}>
            <Button icon={IconNames.CARET_RIGHT} onClick={this.handleRun} text={runButtonText} />
          </Tooltip>
        ) : (
          <Button icon={IconNames.CARET_RIGHT} text={runButtonText} disabled />
        )}
        <Popover position={Position.BOTTOM_LEFT} content={this.renderExtraMenu()}>
          <Button icon={IconNames.MORE} />
        </Popover>
      </ButtonGroup>
    );
  }
}
