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
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
  useHotkeys,
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

const RunButtonExtraMenu = (props: RunButtonProps) => {
  const {
    runeMode,
    onExplain,
    queryContext,
    onQueryContextChange,
    onEditContext,
    onHistory,
    onPrettier,
  } = props;

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
};

export const RunButton = React.memo(function RunButton(props: RunButtonProps) {
  const { runeMode, onRun, loading } = props;

  const handleRun = React.useCallback(() => {
    if (!onRun) return;
    onRun();
  }, [onRun]);

  const hotkeys = React.useMemo(() => {
    return [
      {
        allowInInput: true,
        global: true,
        combo: 'ctrl + enter',
        label: 'Runs the current query',
        onKeyDown: handleRun,
      },
    ];
  }, [handleRun]);

  useHotkeys(hotkeys);

  return (
    <ButtonGroup className="run-button">
      {onRun ? (
        <Button
          className={runeMode ? 'rune-button' : undefined}
          disabled={loading}
          icon={IconNames.CARET_RIGHT}
          onClick={handleRun}
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
      <Popover position={Position.BOTTOM_LEFT} content={<RunButtonExtraMenu {...props} />}>
        <Button
          className={runeMode ? 'rune-button' : undefined}
          icon={IconNames.MORE}
          intent={onRun ? Intent.PRIMARY : undefined}
        />
      </Popover>
    </ButtonGroup>
  );
});
