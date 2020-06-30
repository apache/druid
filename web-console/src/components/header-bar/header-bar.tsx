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
  MenuItem,
  Navbar,
  NavbarDivider,
  NavbarGroup,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import {
  AboutDialog,
  CoordinatorDynamicConfigDialog,
  DoctorDialog,
  OverlordDynamicConfigDialog,
} from '../../dialogs';
import { getLink } from '../../links';
import { Capabilities } from '../../utils/capabilities';
import { ExternalLink } from '../external-link/external-link';
import { PopoverText } from '../popover-text/popover-text';

import './header-bar.scss';

export type HeaderActiveTab =
  | null
  | 'load-data'
  | 'ingestion'
  | 'datasources'
  | 'segments'
  | 'services'
  | 'query'
  | 'lookups';

const DruidLogo = React.memo(function DruidLogo() {
  return (
    <div className="druid-logo">
      <svg
        version="1.1"
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 288 134"
        width="288"
        height="134"
      >
        <path
          fill="#FFFFFF"
          d="M136.7,67.5c0.5-6.1,5-10.4,10.6-10.4c3.9,0,6.5,2,7.4,4.3l1.1-12.4c0-0.1,0.3-0.2,0.7-0.2
                c0.7,0,1.3,0.4,1.2,2l-2.3,25.9c-0.1,0.7-0.5,1-1,1h-0.2c-0.6,0-0.9-0.3-0.8-1l0.3-3.2c-1.7,2.7-4.5,4.5-8.3,4.5
                C139.9,77.9,136.2,73.7,136.7,67.5z M154,68.9l0.4-4.7c-0.9-3.3-3.3-5.4-7.2-5.4c-4.5,0-8.1,3.6-8.5,8.6
                c-0.4,5.1,2.5,8.7,6.9,8.7C150,76.1,153.7,72.9,154,68.9z"
        />
        <path
          fill="#FFFFFF"
          d="M161.2,76.6l1.7-19.1c0,0,0.3-0.2,0.7-0.2c0.7,0,1.3,0.4,1.1,2l-0.2,2.5c1.1-3.3,3.3-4.8,6-4.8
                c1.6,0,2.7,0.7,2.6,1.7c-0.1,0.8-0.6,1.1-0.7,1.1c-0.5-0.5-1.3-0.8-2.3-0.8c-3.6,0-5.6,3.6-6.1,9l-0.8,8.7c-0.1,0.7-0.5,1-1,1
                h-0.2C161.5,77.6,161.2,77.4,161.2,76.6z"
        />
        <path
          fill="#FFFFFF"
          d="M175.6,69l0.9-10.7c0.1-0.8,0.5-1,1-1h0.3c0.5,0,0.9,0.2,0.8,1l-0.9,10.5c-0.4,4.4,1.5,7.2,5.5,7.2
                c3.3,0,6-1.9,7.5-4.7l1.1-13c0.1-0.8,0.5-1,1-1h0.3c0.5,0,0.9,0.2,0.8,1l-1.7,19.1c0,0-0.4,0.2-0.7,0.2c-0.7,0-1.2-0.4-1.1-2
                l0.2-1.8c-1.6,2.4-4.2,4.1-7.6,4.1C177.6,77.9,175.2,74.4,175.6,69z"
        />
        <path
          fill="#FFFFFF"
          d="M200.1,50.7c0.1-1,0.6-1.4,1.6-1.4c0.9,0,1.4,0.5,1.3,1.4c-0.1,0.9-0.6,1.4-1.6,1.4
                C200.5,52.1,200,51.6,200.1,50.7z M198.2,76.6l1.6-18.3c0.1-0.8,0.5-1,1-1h0.3c0.5,0,0.9,0.2,0.8,1l-1.6,18.3
                c-0.1,0.8-0.5,1-1,1H199C198.5,77.6,198.2,77.4,198.2,76.6z"
        />
        <path
          fill="#FFFFFF"
          d="M205.8,67.5c0.5-6.1,5-10.4,10.6-10.4c3.9,0,6.5,2,7.4,4.3l1.1-12.4c0-0.1,0.3-0.2,0.7-0.2
                c0.7,0,1.3,0.4,1.2,2l-2.3,25.9c-0.1,0.7-0.5,1-1,1h-0.2c-0.5,0-0.9-0.3-0.8-1l0.3-3.2c-1.7,2.7-4.5,4.5-8.3,4.5
                C209,77.9,205.2,73.7,205.8,67.5z M223.1,68.9l0.4-4.7c-0.9-3.3-3.3-5.4-7.2-5.4c-4.5,0-8.1,3.6-8.5,8.6
                c-0.4,5.1,2.5,8.7,6.9,8.7C219,76.1,222.7,72.9,223.1,68.9z"
        />
        <path
          fill="#2CEEFB"
          d="M96.2,89.8h-2.7c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3h2.7c11.5,0,23.8-7.4,23.8-23.7
                c0-9.1-6.9-15.8-16.4-15.8H80c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3h23.6c5.3,0,10.1,1.9,13.6,5.3
                c3.5,3.4,5.4,8,5.4,13.1c0,6.6-2.3,13-6.3,17.7C111.5,86.8,104.5,89.8,96.2,89.8z M87.1,89.8h-5.8c-0.7,0-1.3-0.6-1.3-1.3
                c0-0.7,0.6-1.3,1.3-1.3h5.8c0.7,0,1.3,0.6,1.3,1.3C88.4,89.2,87.8,89.8,87.1,89.8z M97.7,79.5h-26c-0.7,0-1.3-0.6-1.3-1.3
                c0-0.7,0.6-1.3,1.3-1.3h26c7.5,0,11.5-5.8,11.5-11.5c0-4.2-3.2-7.3-7.7-7.3h-26c-0.7,0-1.3-0.6-1.3-1.3c0-0.7,0.6-1.3,1.3-1.3
                h26c5.9,0,10.3,4.3,10.3,9.9c0,3.7-1.3,7.2-3.7,9.8C105.5,78,101.9,79.5,97.7,79.5z M69.2,58h-6.3c-0.7,0-1.3-0.6-1.3-1.3
                c0-0.7,0.6-1.3,1.3-1.3h6.3c0.7,0,1.3,0.6,1.3,1.3C70.5,57.4,69.9,58,69.2,58z"
        />
      </svg>
    </div>
  );
});

interface RestrictedModeProps {
  capabilities: Capabilities;
}

const RestrictedMode = React.memo(function RestrictedMode(props: RestrictedModeProps) {
  const { capabilities } = props;
  const mode = capabilities.getModeExtended();

  let label: string;
  let message: JSX.Element;
  switch (mode) {
    case 'full':
      return null; // Do not show anything

    case 'no-sql':
      label = 'No SQL mode';
      message = (
        <p>
          It appears that the SQL endpoint is disabled. The console will fall back to{' '}
          <ExternalLink href={getLink('DOCS_API')}>native Druid APIs</ExternalLink> and will be
          limited in functionality. Look at{' '}
          <ExternalLink href={getLink('DOCS_SQL')}>the SQL docs</ExternalLink> to enable the SQL
          endpoint.
        </p>
      );
      break;

    case 'no-proxy':
      label = 'No management proxy mode';
      message = (
        <p>
          It appears that the management proxy is not enabled, the console will operate with limited
          functionality.
        </p>
      );
      break;

    case 'no-sql-no-proxy':
      label = 'No SQL mode';
      message = (
        <p>
          It appears that the SQL endpoint and management proxy are disabled. The console can only
          be used to make queries.
        </p>
      );
      break;

    case 'coordinator':
      label = 'Coordinator mode';
      message = (
        <p>
          It appears that you are accessing the console on the Coordinator service. Due to the lack
          of access to some APIs on this service the console will operate in a limited mode. The
          full version of the console can be accessed on the Router service.
        </p>
      );
      break;

    case 'overlord':
      label = 'Overlord mode';
      message = (
        <p>
          It appears that you are accessing the console on the Overlord service. Due to the lack of
          access to some APIs on this service the console will operate in a limited mode. The full
          version of the console can be accessed on the Router service.
        </p>
      );
      break;

    default:
      label = 'Restricted mode';
      message = (
        <p>
          Due to the lack of access to some APIs on this service the console will operate in a
          limited mode. The full version of the console can be accessed on the Router service.
        </p>
      );
      break;
  }

  return (
    <Popover
      content={
        <PopoverText>
          <p>The console is running in restricted mode.</p>
          {message}
          <p>
            For more info check out the{' '}
            <ExternalLink href={`${getLink('DOCS')}/operations/management-uis.html#druid-console`}>
              console documentation
            </ExternalLink>
            .
          </p>
        </PopoverText>
      }
      position={Position.BOTTOM_RIGHT}
    >
      <Button icon={IconNames.WARNING_SIGN} text={label} intent={Intent.WARNING} minimal />
    </Popover>
  );
});

export interface HeaderBarProps {
  active: HeaderActiveTab;
  capabilities: Capabilities;
}

export const HeaderBar = React.memo(function HeaderBar(props: HeaderBarProps) {
  const { active, capabilities } = props;
  const [aboutDialogOpen, setAboutDialogOpen] = useState(false);
  const [doctorDialogOpen, setDoctorDialogOpen] = useState(false);
  const [coordinatorDynamicConfigDialogOpen, setCoordinatorDynamicConfigDialogOpen] = useState(
    false,
  );
  const [overlordDynamicConfigDialogOpen, setOverlordDynamicConfigDialogOpen] = useState(false);
  const loadDataPrimary = false;

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
        text="ASF Slack channel"
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
        icon={IconNames.PROPERTIES}
        active={active === 'lookups'}
        text="Lookups"
        href="#lookups"
        disabled={!capabilities.hasCoordinatorAccess()}
      />
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
          icon={IconNames.CLOUD_UPLOAD}
          text="Load data"
          active={active === 'load-data'}
          href="#load-data"
          minimal={!loadDataPrimary}
          intent={loadDataPrimary ? Intent.PRIMARY : Intent.NONE}
          disabled={!capabilities.hasEverything()}
        />

        <NavbarDivider />
        <AnchorButton
          minimal
          active={active === 'ingestion'}
          icon={IconNames.GANTT_CHART}
          text="Ingestion"
          href="#ingestion"
          disabled={!capabilities.hasSqlOrOverlordAccess()}
        />
        <AnchorButton
          minimal
          active={active === 'datasources'}
          icon={IconNames.MULTI_SELECT}
          text="Datasources"
          href="#datasources"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          minimal
          active={active === 'segments'}
          icon={IconNames.STACKED_CHART}
          text="Segments"
          href="#segments"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          minimal
          active={active === 'services'}
          icon={IconNames.DATABASE}
          text="Services"
          href="#services"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />

        <NavbarDivider />
        <AnchorButton
          minimal
          active={active === 'query'}
          icon={IconNames.APPLICATION}
          text="Query"
          href="#query"
          disabled={!capabilities.hasQuerying()}
        />
      </NavbarGroup>
      <NavbarGroup align={Alignment.RIGHT}>
        <RestrictedMode capabilities={capabilities} />
        <Popover content={configMenu} position={Position.BOTTOM_RIGHT}>
          <Button minimal icon={IconNames.COG} />
        </Popover>
        <Popover content={helpMenu} position={Position.BOTTOM_RIGHT}>
          <Button minimal icon={IconNames.HELP} />
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
    </Navbar>
  );
});
