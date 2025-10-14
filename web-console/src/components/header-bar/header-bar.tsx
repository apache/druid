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
  AnchorButton,
  Button,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Navbar,
  NavbarDivider,
  NavbarGroup,
  Popover,
  Position,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import {
  AboutDialog,
  CompactionDynamicConfigDialog,
  CoordinatorDynamicConfigDialog,
  DoctorDialog,
  OverlordDynamicConfigDialog,
} from '../../dialogs';
import { WebConsoleConfigDialog } from '../../dialogs/web-console-config-dialog/web-console-config-dialog';
import type { ConsoleViewId } from '../../druid-models';
import { getConsoleViewIcon } from '../../druid-models';
import { Capabilities } from '../../helpers';
import { getLink } from '../../links';
import {
  EXPERIMENTAL_ICON,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageRemove,
  localStorageSetJson,
  oneOf,
} from '../../utils';
import { DruidLogo } from '../druid-logo/druid-logo';
import { PopoverText } from '../popover-text/popover-text';

import { RestrictedMode } from './restricted-mode/restricted-mode';

import './header-bar.scss';

const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);

export interface HeaderBarProps {
  activeView: ConsoleViewId | null;
  capabilities: Capabilities;
  onUnrestrict(capabilities: Capabilities): void;
}

export const HeaderBar = React.memo(function HeaderBar(props: HeaderBarProps) {
  const { activeView, capabilities, onUnrestrict } = props;
  const [aboutDialogOpen, setAboutDialogOpen] = useState(false);
  const [doctorDialogOpen, setDoctorDialogOpen] = useState(false);
  const [coordinatorDynamicConfigDialogOpen, setCoordinatorDynamicConfigDialogOpen] =
    useState(false);
  const [overlordDynamicConfigDialogOpen, setOverlordDynamicConfigDialogOpen] = useState(false);
  const [compactionDynamicConfigDialogOpen, setCompactionDynamicConfigDialogOpen] = useState(false);
  const [webConsoleConfigDialogOpen, setWebConsoleConfigDialogOpen] = useState(false);

  const showSplitDataLoaderMenu = capabilities.hasMultiStageQueryTask();

  const loadDataViewsMenuActive = oneOf(
    activeView,
    'data-loader',
    'streaming-data-loader',
    'classic-batch-data-loader',
    'sql-data-loader',
  );
  const loadDataViewsMenu = (
    <Menu>
      <MenuItem
        icon={getConsoleViewIcon('streaming-data-loader')}
        text="Streaming"
        href="#streaming-data-loader"
        selected={activeView === 'streaming-data-loader'}
      />
      <MenuItem
        icon={getConsoleViewIcon('sql-data-loader')}
        text="Batch - SQL"
        href="#sql-data-loader"
        labelElement={<Tag minimal>multi-stage-query</Tag>}
        selected={activeView === 'sql-data-loader'}
      />
      <MenuItem
        icon={getConsoleViewIcon('classic-batch-data-loader')}
        text="Batch - classic"
        href="#classic-batch-data-loader"
        selected={activeView === 'classic-batch-data-loader'}
      />
    </Menu>
  );

  const moreViewsMenuActive = oneOf(activeView, 'lookups');
  const moreViewsMenu = (
    <Menu>
      <MenuItem
        icon={getConsoleViewIcon('lookups')}
        text="Lookups"
        href="#lookups"
        disabled={!capabilities.hasCoordinatorAccess()}
        selected={activeView === 'lookups'}
      />
      <MenuDivider />
      <MenuItem
        icon={getConsoleViewIcon('explore')}
        text="Explore"
        labelElement={EXPERIMENTAL_ICON}
        href="#explore"
        disabled={!capabilities.hasSql()}
        selected={activeView === 'explore'}
        target="_blank"
      />
    </Menu>
  );

  const helpMenu = (
    <Menu>
      <MenuItem icon={IconNames.GRAPH} text="About" onClick={() => setAboutDialogOpen(true)} />
      <MenuItem icon={IconNames.TH} text="Docs" href={getLink('DOCS')} target="_blank" />
      <MenuItem
        icon={IconNames.USER}
        text="User group"
        href={getLink('USER_GROUP')}
        target="_blank"
      />
      <MenuItem
        icon={IconNames.CHAT}
        text="Slack channel"
        href={getLink('SLACK')}
        target="_blank"
      />
      <MenuItem
        icon={IconNames.GIT_BRANCH}
        text="GitHub"
        href={getLink('GITHUB')}
        target="_blank"
      />
    </Menu>
  );

  function setCapabilitiesOverride(capabilities: Capabilities | undefined): void {
    if (capabilities) {
      localStorageSetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE, capabilities);
    } else {
      localStorageRemove(LocalStorageKeys.CAPABILITIES_OVERRIDE);
    }
    location.reload();
  }

  const capabilitiesMode = capabilities.getModeExtended();
  const configMenu = (
    <Menu>
      <MenuItem
        icon={IconNames.PULSE}
        text="Druid Doctor"
        onClick={() => setDoctorDialogOpen(true)}
        disabled={!capabilities.hasEverything()}
      />
      <MenuItem
        icon={IconNames.SETTINGS}
        text="Coordinator dynamic config"
        onClick={() => setCoordinatorDynamicConfigDialogOpen(true)}
        disabled={!capabilities.hasCoordinatorAccess()}
      />
      <MenuItem
        icon={IconNames.WRENCH}
        text="Overlord dynamic config"
        onClick={() => setOverlordDynamicConfigDialogOpen(true)}
        disabled={!capabilities.hasOverlordAccess()}
      />
      <MenuItem
        icon={IconNames.COMPRESSED}
        text="Compaction dynamic config"
        onClick={() => setCompactionDynamicConfigDialogOpen(true)}
        disabled={!capabilities.hasCoordinatorAccess()}
      />
      <MenuItem
        icon={IconNames.CONSOLE}
        text="Web console config"
        onClick={() => setWebConsoleConfigDialogOpen(true)}
      />
      <MenuDivider />
      <MenuItem
        icon={IconNames.HIGH_PRIORITY}
        text="Capabilty detection"
        intent={capabilitiesOverride ? Intent.DANGER : undefined}
      >
        {capabilitiesOverride && (
          <>
            <MenuItem
              text="Use automatic capabilty detection"
              onClick={() => setCapabilitiesOverride(undefined)}
              intent={Intent.PRIMARY}
            />
            <MenuDivider />
          </>
        )}
        {capabilitiesMode !== 'coordinator-overlord' && (
          <MenuItem
            text="Manually set Coordinator/Overlord mode"
            onClick={() => setCapabilitiesOverride(Capabilities.COORDINATOR_OVERLORD)}
          />
        )}
        {capabilitiesMode !== 'coordinator' && (
          <MenuItem
            text="Manually set Coordinator mode"
            onClick={() => setCapabilitiesOverride(Capabilities.COORDINATOR)}
          />
        )}
        {capabilitiesMode !== 'overlord' && (
          <MenuItem
            text="Manually set Overlord mode"
            onClick={() => setCapabilitiesOverride(Capabilities.OVERLORD)}
          />
        )}
        {capabilitiesMode !== 'no-proxy' && (
          <MenuItem
            text="Manually set Router with no management proxy mode"
            onClick={() => setCapabilitiesOverride(Capabilities.NO_PROXY)}
          />
        )}
      </MenuItem>
    </Menu>
  );

  return (
    <Navbar className="header-bar">
      <NavbarGroup align={Alignment.LEFT}>
        <a href="#">
          <DruidLogo />
        </a>
        <NavbarDivider />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'workbench'}
          icon={getConsoleViewIcon('workbench')}
          text="Query"
          href="#workbench"
          disabled={!capabilities.hasQuerying()}
        />
        {showSplitDataLoaderMenu ? (
          <Popover
            content={loadDataViewsMenu}
            disabled={!capabilities.hasEverything()}
            position={Position.BOTTOM_LEFT}
          >
            <Button
              className="header-entry"
              icon={getConsoleViewIcon('data-loader')}
              text="Load data"
              minimal
              active={loadDataViewsMenuActive}
              disabled={!capabilities.hasEverything()}
            />
          </Popover>
        ) : (
          <AnchorButton
            className="header-entry"
            icon={getConsoleViewIcon('data-loader')}
            text="Load data"
            href="#data-loader"
            minimal
            active={loadDataViewsMenuActive}
            disabled={!capabilities.hasEverything()}
          />
        )}
        <NavbarDivider />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'datasources'}
          icon={getConsoleViewIcon('datasources')}
          text="Datasources"
          href="#datasources"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'supervisors'}
          icon={getConsoleViewIcon('supervisors')}
          text="Supervisors"
          href="#supervisors"
          disabled={!capabilities.hasSqlOrOverlordAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'tasks'}
          icon={getConsoleViewIcon('tasks')}
          text="Tasks"
          href="#tasks"
          disabled={!capabilities.hasSqlOrOverlordAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'segments'}
          icon={getConsoleViewIcon('segments')}
          text="Segments"
          href="#segments"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={activeView === 'services'}
          icon={getConsoleViewIcon('services')}
          text="Services"
          href="#services"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <Popover content={moreViewsMenu} position={Position.BOTTOM_LEFT}>
          <Button
            className="header-entry"
            minimal
            icon={IconNames.MORE}
            active={moreViewsMenuActive}
            data-tooltip="More views"
          />
        </Popover>
      </NavbarGroup>
      <NavbarGroup align={Alignment.RIGHT}>
        <RestrictedMode
          capabilities={capabilities}
          onUnrestrict={onUnrestrict}
          onUseAutomaticCapabilityDetection={
            capabilitiesOverride ? () => setCapabilitiesOverride(undefined) : undefined
          }
        />
        {capabilitiesOverride && (
          <Popover
            content={
              <PopoverText>
                <p>
                  The console is running in a manual capability setting mode that assumes a limited
                  set of capabilities rather than detecting all capabilities for the cluster.
                </p>
                <p>
                  Manual capability setting mode is an advanced feature used for testing and for
                  working around issues with the automatic capability detecting logic.
                </p>
                <p>
                  If you are unsure why the console is in this mode, revert to using automatic
                  capability detection.
                </p>
                <p>
                  <Button
                    text="Use automatic capability detection"
                    onClick={() => setCapabilitiesOverride(undefined)}
                    intent={Intent.PRIMARY}
                  />
                </p>
              </PopoverText>
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button
              icon={IconNames.HIGH_PRIORITY}
              text="Manual capabilty detection"
              intent={Intent.DANGER}
              minimal
            />
          </Popover>
        )}
        <Popover content={configMenu} position={Position.BOTTOM_RIGHT}>
          <Button className="header-entry" minimal icon={IconNames.COG} data-tooltip="Settings" />
        </Popover>
        <Popover content={helpMenu} position={Position.BOTTOM_RIGHT}>
          <Button className="header-entry" minimal icon={IconNames.HELP} data-tooltip="Help" />
        </Popover>
      </NavbarGroup>
      {aboutDialogOpen && <AboutDialog onClose={() => setAboutDialogOpen(false)} />}
      {doctorDialogOpen && <DoctorDialog onClose={() => setDoctorDialogOpen(false)} />}
      {coordinatorDynamicConfigDialogOpen && (
        <CoordinatorDynamicConfigDialog
          onClose={() => setCoordinatorDynamicConfigDialogOpen(false)}
        />
      )}
      {overlordDynamicConfigDialogOpen && (
        <OverlordDynamicConfigDialog onClose={() => setOverlordDynamicConfigDialogOpen(false)} />
      )}
      {compactionDynamicConfigDialogOpen && (
        <CompactionDynamicConfigDialog
          onClose={() => setCompactionDynamicConfigDialogOpen(false)}
        />
      )}
      {webConsoleConfigDialogOpen && (
        <WebConsoleConfigDialog onClose={() => setWebConsoleConfigDialogOpen(false)} />
      )}
    </Navbar>
  );
});
