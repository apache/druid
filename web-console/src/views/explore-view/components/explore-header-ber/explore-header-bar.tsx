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
  Alignment,
  Button,
  Icon,
  Menu,
  MenuItem,
  Navbar,
  NavbarGroup,
  Popover,
  Position,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Timezone } from 'chronoshift';
import type { JSX } from 'react';
import React from 'react';

import { DruidLogo, TimezoneMenuItems } from '../../../../components';
import { getConsoleViewIcon } from '../../../../druid-models';
import type { Capabilities } from '../../../../helpers';
import { capitalizeFirst } from '../../../../utils';
import { type ExploreModuleLayout, ExploreState, LAYOUT_TO_ICON } from '../../models';

import './explore-header-bar.scss';

export interface ExploreHeaderBarProps {
  capabilities: Capabilities;
  timezone: Timezone | undefined;
  onChangeTimezone(timezone: Timezone | undefined): void;
  moreMenu: JSX.Element;
  layout: ExploreModuleLayout;
  onChangeLayout(layout: ExploreModuleLayout): void;
  onShowHideSidePanel(alt: boolean): void;
}

export const ExploreHeaderBar = React.memo(function ExploreHeaderBar(props: ExploreHeaderBarProps) {
  const {
    capabilities,
    timezone,
    onChangeTimezone,
    moreMenu,
    layout,
    onChangeLayout,
    onShowHideSidePanel,
  } = props;

  const showSplitDataLoaderMenu = capabilities.hasMultiStageQueryTask();

  return (
    <Navbar className="explore-header-bar">
      <NavbarGroup align={Alignment.LEFT}>
        <Popover
          position={Position.BOTTOM_RIGHT}
          content={
            <Menu>
              <MenuItem icon={IconNames.HOME} text="Home" href="#" />
              <MenuItem
                icon={getConsoleViewIcon('workbench')}
                text="Query"
                href="#workbench"
                disabled={!capabilities.hasQuerying()}
              />
              {showSplitDataLoaderMenu ? (
                <MenuItem
                  icon={getConsoleViewIcon('data-loader')}
                  text="Load data"
                  disabled={!capabilities.hasEverything()}
                >
                  <MenuItem
                    icon={getConsoleViewIcon('streaming-data-loader')}
                    text="Streaming"
                    href="#streaming-data-loader"
                  />
                  <MenuItem
                    icon={getConsoleViewIcon('sql-data-loader')}
                    text="Batch - SQL"
                    href="#sql-data-loader"
                    labelElement={<Tag minimal>multi-stage-query</Tag>}
                  />
                  <MenuItem
                    icon={getConsoleViewIcon('classic-batch-data-loader')}
                    text="Batch - classic"
                    href="#classic-batch-data-loader"
                  />
                </MenuItem>
              ) : (
                <MenuItem
                  icon={getConsoleViewIcon('data-loader')}
                  text="Load data"
                  href="#data-loader"
                  disabled={!capabilities.hasEverything()}
                />
              )}
              <MenuItem
                icon={getConsoleViewIcon('datasources')}
                text="Datasources"
                href="#datasources"
                disabled={!capabilities.hasSqlOrCoordinatorAccess()}
              />
              <MenuItem
                icon={getConsoleViewIcon('supervisors')}
                text="Supervisors"
                href="#supervisors"
                disabled={!capabilities.hasSqlOrOverlordAccess()}
              />
              <MenuItem
                icon={getConsoleViewIcon('tasks')}
                text="Tasks"
                href="#tasks"
                disabled={!capabilities.hasSqlOrOverlordAccess()}
              />
              <MenuItem
                icon={getConsoleViewIcon('segments')}
                text="Segments"
                href="#segments"
                disabled={!capabilities.hasSqlOrCoordinatorAccess()}
              />
              <MenuItem
                icon={getConsoleViewIcon('services')}
                text="Services"
                href="#services"
                disabled={!capabilities.hasSqlOrCoordinatorAccess()}
              />
            </Menu>
          }
        >
          <div className="explore-logo">
            <DruidLogo compact />
            <span className="explore-logo-text">Explore</span>
          </div>
        </Popover>
      </NavbarGroup>
      <NavbarGroup align={Alignment.RIGHT}>
        <Popover
          position={Position.BOTTOM_RIGHT}
          content={
            <Menu>
              <TimezoneMenuItems
                sqlTimeZone={timezone?.toString()}
                setSqlTimeZone={timezone =>
                  onChangeTimezone(timezone ? Timezone.fromJS(timezone) : undefined)
                }
                defaultSqlTimeZone="Etc/UTC"
                namedOnly
              />
            </Menu>
          }
        >
          <Button
            className="header-entry"
            icon={IconNames.GLOBE_NETWORK}
            text={timezone ? timezone.toString() : 'Etc/UTC'}
            data-tooltip="Change query timezone"
            minimal
          />
        </Popover>
        <Popover position={Position.BOTTOM_RIGHT} content={moreMenu}>
          <Button
            className="header-entry"
            icon={IconNames.MORE}
            data-tooltip="More options"
            minimal
          />
        </Popover>
        <Popover
          position={Position.BOTTOM_RIGHT}
          content={
            <Menu>
              {ExploreState.LAYOUTS.map(l => (
                <MenuItem
                  key={l}
                  icon={LAYOUT_TO_ICON[l]}
                  text={capitalizeFirst(l.replace(/-/g, ' '))}
                  labelElement={layout === l ? <Icon icon={IconNames.TICK} /> : undefined}
                  onClick={() => {
                    onChangeLayout(l);
                  }}
                />
              ))}
            </Menu>
          }
        >
          <Button className="header-entry" icon={IconNames.CONTROL} data-tooltip="Layout" minimal />
        </Popover>
        <Button
          className="header-entry"
          minimal
          icon={IconNames.PANEL}
          data-tooltip="Show/hide side panels"
          onClick={e => onShowHideSidePanel(e.altKey)}
        />
      </NavbarGroup>
    </Navbar>
  );
});
