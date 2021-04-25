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
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { MenuCheckbox } from '../../../components';
import { getLink } from '../../../links';
import { pluralIfNeeded } from '../../../utils';
import {
  getUseApproximateCountDistinct,
  getUseApproximateTopN,
  getUseCache,
  QueryContext,
  setUseApproximateCountDistinct,
  setUseApproximateTopN,
  setUseCache,
} from '../../../utils/query-context';

import './run-button.scss';

export interface RunButtonProps {
  runeMode: boolean;
  queryContext: QueryContext;
  onQueryContextChange: (newQueryContext: QueryContext) => void;
  onRun: (() => void) | undefined;
  loading: boolean;
  onExplain: (() => void) | undefined;
  onEditContext: () => void;
  onHistory: () => void;
  onPrettier: () => void;
}

@HotkeysTarget
export class RunButton extends React.PureComponent<RunButtonProps> {
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

  private readonly handleRun = () => {
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
      onPrettier,
    } = this.props;

    const useCache = getUseCache(queryContext);
    const useApproximateCountDistinct = getUseApproximateCountDistinct(queryContext);
    const useApproximateTopN = getUseApproximateTopN(queryContext);
    const numContextKeys = Object.keys(queryContext).length;

    return (
      <Menu>
        <MenuItem
          icon={IconNames.HELP}
          text={runeMode ? 'Native query documentation' : 'DruidSQL documentation'}
          href={getLink(runeMode ? 'DOCS_RUNE' : 'DOCS_SQL')}
          target="_blank"
        />
        <MenuItem icon={IconNames.HISTORY} text="Query history" onClick={onHistory} />
        {!runeMode && onExplain && (
          <MenuItem icon={IconNames.CLEAN} text="Explain SQL query" onClick={onExplain} />
        )}
        {runeMode && (
          <MenuItem icon={IconNames.ALIGN_LEFT} text="Prettify JSON" onClick={onPrettier} />
        )}
        <MenuItem
          icon={IconNames.PROPERTIES}
          text="Edit context"
          onClick={onEditContext}
          label={numContextKeys ? pluralIfNeeded(numContextKeys, 'key') : undefined}
        />
        <MenuDivider />
        {!runeMode && (
          <>
            <MenuCheckbox
              checked={useApproximateCountDistinct}
              text="Use approximate COUNT(DISTINCT)"
              onChange={() => {
                onQueryContextChange(
                  setUseApproximateCountDistinct(queryContext, !useApproximateCountDistinct),
                );
              }}
            />
            <MenuCheckbox
              checked={useApproximateTopN}
              text="Use approximate TopN"
              onChange={() => {
                onQueryContextChange(setUseApproximateTopN(queryContext, !useApproximateTopN));
              }}
            />
          </>
        )}
        <MenuCheckbox
          checked={useCache}
          text="Use cache"
          onChange={() => {
            onQueryContextChange(setUseCache(queryContext, !useCache));
          }}
        />
      </Menu>
    );
  }

  render(): JSX.Element {
    const { runeMode, onRun, loading } = this.props;

    return (
      <ButtonGroup className="run-button">
        {onRun ? (
          <Button
            className={runeMode ? 'rune-button' : undefined}
            disabled={loading}
            icon={IconNames.CARET_RIGHT}
            onClick={this.handleRun}
            text="Run"
            intent={Intent.PRIMARY}
          />
        ) : (
          <Button
            className={runeMode ? 'rune-button' : undefined}
            icon={IconNames.CARET_RIGHT}
            text="Run"
            disabled
          />
        )}
        <Popover position={Position.BOTTOM_LEFT} content={this.renderExtraMenu()}>
          <Button
            className={runeMode ? 'rune-button' : undefined}
            icon={IconNames.MORE}
            intent={onRun ? Intent.PRIMARY : undefined}
          />
        </Popover>
      </ButtonGroup>
    );
  }
}
