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
  Position,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import {
  AboutDialog,
  CoordinatorDynamicConfigDialog,
  DoctorDialog,
  OverlordDynamicConfigDialog,
} from '../../dialogs';
import { getLink } from '../../links';
import {
  Capabilities,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageRemove,
  localStorageSetJson,
  oneOf,
} from '../../utils';
import { ExternalLink } from '../external-link/external-link';
import { PopoverText } from '../popover-text/popover-text';

import './header-bar.scss';

const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);

export type HeaderActiveTab =
  | null
  | 'data-loader'
  | 'streaming-data-loader'
  | 'classic-batch-data-loader'
  | 'ingestion'
  | 'datasources'
  | 'segments'
  | 'services'
  | 'workbench'
  | 'sql-data-loader'
  | 'lookups';

const DruidLogo = React.memo(function DruidLogo() {
  return (
    <div className="druid-logo">
      <svg xmlns="http://www.w3.org/2000/svg" width="300" height="150" viewBox="0 0 300 150">
        <path
          className="letter"
          fill="#FFFFFF"
          d="M165.522007,43.8777377 C167.235269,43.8777377 168.153205,45.0195075 168.009772,47.4341706 L167.990331,47.6977613 L164.355619,89.2407621 C164.241296,90.5455031 163.497711,91.2519433 162.346343,91.3226912 L162.15071,91.3285901 L161.810207,91.3285901 C160.582115,91.3285901 159.889116,90.6340734 159.933577,89.3390588 L159.945802,89.1403146 L160.21785,86.036451 L160.003069,86.293478 C157.156477,89.623947 153.271886,91.5639998 148.542368,91.7844956 L148.066606,91.8008011 L147.627951,91.805615 C138.410258,91.805615 132.332827,84.5972475 133.225974,74.3863688 C134.106209,64.3251285 141.469265,57.1348544 150.764751,57.1348544 C155.668986,57.1348544 159.689966,59.1295932 161.961648,62.2288522 L162.163321,62.513621 L162.262333,62.6635387 L163.830809,44.746288 C163.873679,44.2257202 164.475336,43.9550921 165.187241,43.8921792 L165.353279,43.8813288 L165.522007,43.8777377 Z M199.742522,57.6102732 C200.949183,57.6102732 201.663002,58.2778902 201.617832,59.5972551 L201.60533,59.800046 L200.124466,76.7304108 C199.522093,83.6177963 202.488621,87.5953849 208.337001,87.5953849 C213.183439,87.5953849 217.262823,85.0358707 219.655314,80.7034929 L219.787424,80.4564055 L221.604097,59.6995339 C221.715268,58.4351741 222.353614,57.7607868 223.368105,57.6328201 L223.551007,57.615901 L223.741533,57.6102732 L224.149494,57.6102732 C225.357447,57.6102732 226.071581,58.2775756 226.026412,59.5972118 L226.013908,59.8000478 L223.331244,90.4689713 C223.279804,91.0090843 222.362486,91.3285901 221.573238,91.3285901 C219.923489,91.3285901 219.073518,90.170171 219.224638,87.708057 L219.24467,87.4392638 L219.324913,86.5162199 L219.092124,86.784537 C216.45535,89.7293932 212.833628,91.5581276 208.723878,91.7822915 L208.281703,91.8001469 L207.864795,91.805615 C199.401817,91.805615 195.013144,86.2666181 195.664934,77.2348691 L195.696963,76.83415 L197.195519,59.6995339 C197.313229,58.3608 198.02196,57.6834886 199.142429,57.615901 L199.332955,57.6102732 L199.742522,57.6102732 Z M276.41183,43.8777377 C278.126074,43.8777377 279.043144,45.0189522 278.899603,47.4341132 L278.880153,47.6977613 L275.245442,89.2407621 C275.131118,90.5455031 274.387534,91.2519433 273.236165,91.3226912 L273.040532,91.3285901 L272.70003,91.3285901 C271.472159,91.3285901 270.780407,90.6341867 270.824982,89.3392277 L270.837215,89.1404914 L271.107736,86.0353005 L270.892896,86.2933545 C268.046135,89.6236752 264.160512,91.5639676 259.430642,91.7844927 L258.954848,91.8008005 L258.516168,91.805615 C249.29984,91.805615 243.222674,84.5969779 244.115796,74.3863688 C244.996032,64.3251285 252.359088,57.1348544 261.654574,57.1348544 C266.558559,57.1348544 270.579401,59.1293342 272.852124,62.2286026 L273.053895,62.513373 L273.153369,62.6635387 L274.720632,44.746288 C274.763502,44.2257202 275.365159,43.9550921 276.077063,43.8921792 L276.243102,43.8813288 L276.41183,43.8777377 Z M237.135172,57.6102732 C238.342471,57.6102732 239.05551,58.2770666 239.011843,59.5971566 L238.999585,59.8000681 L236.423373,89.2401891 C236.307413,90.5790035 235.598059,91.2554713 234.476571,91.3229698 L234.285872,91.3285901 L233.877911,91.3285901 C232.670326,91.3285901 231.95755,90.6632026 232.001239,89.343295 L232.013498,89.1404013 L234.58971,59.7002803 C234.70568,58.361348 235.41539,57.6835442 236.536569,57.6159054 L236.727211,57.6102732 L237.135172,57.6102732 Z M188.022486,57.1348544 C190.89175,57.1348544 193.020533,58.4576694 192.845401,60.4796404 C192.734257,61.7453607 191.741347,62.9260612 191.000795,62.6810692 L190.905726,62.6409183 L190.764193,62.5374342 C190.07468,61.8493976 188.868364,61.4125426 187.408938,61.4125426 C182.329143,61.4125426 179.109026,66.2704719 178.263765,74.8476891 L177.002401,89.2407621 C176.889186,90.5328541 176.154042,91.2506119 175.05198,91.3225861 L174.86495,91.3285901 L174.524447,91.3285901 C173.283533,91.3285901 172.53817,90.6270545 172.581054,89.3297867 L172.592575,89.1404216 L175.275239,58.4714981 C175.326046,57.9380285 176.19005,57.6102732 176.965787,57.6102732 C178.671987,57.6102732 179.53354,58.7543734 179.380524,61.2289962 L179.360239,61.4992342 L179.300455,62.1699636 L179.442449,61.9373161 C181.306424,58.9476372 183.982279,57.3464392 187.329545,57.1544624 L187.690743,57.1392869 L188.022486,57.1348544 Z M150.718173,61.1411042 C143.74725,61.1411042 138.323314,66.6200602 137.640629,74.4175294 C136.955381,82.2577516 141.366437,87.7993652 148.217406,87.7993652 C154.670267,87.7993652 160.298092,83.0285379 160.996168,77.0792595 L161.028335,76.7650709 L161.679016,69.3308302 L161.605859,69.086919 C160.048572,64.1145742 156.45606,61.2693345 151.086996,61.1453349 L150.718173,61.1411042 Z M261.607996,61.1411042 C254.637073,61.1411042 249.213136,66.6200602 248.530451,74.4175294 C247.845204,82.2577516 252.25626,87.7993652 259.107229,87.7993652 C265.560409,87.7993652 271.189504,83.0282382 271.887597,77.079244 L271.919764,76.7650709 L272.570053,69.3285292 L272.497294,69.0869478 C270.940073,64.1150052 267.346304,61.2693537 261.976844,61.1453356 L261.607996,61.1411042 Z M238.142225,44.5571368 C239.987031,44.5571368 240.92155,45.6068176 240.756683,47.4940596 C240.598952,49.2868072 239.470558,50.2626875 237.607379,50.2626875 C235.828351,50.2626875 234.895657,49.2146035 235.053974,47.3929932 C235.21836,45.5331321 236.279915,44.5571368 238.142225,44.5571368 Z"
        />
        <path
          className="swirl"
          fill="#29F1FB"
          d="M54.1037631,105.568142 C55.5843204,105.568142 56.7862871,106.770109 56.7862871,108.250666 C56.7862871,109.670606 55.6828448,110.832632 54.2873709,110.927002 L54.1037631,110.93319 L44.7431529,110.93319 C43.2621086,110.93319 42.0606289,109.732343 42.0606289,108.250666 C42.0606289,106.831798 43.1645185,105.668789 44.5596011,105.574335 L44.7431529,105.568142 L54.1037631,105.568142 Z M80.5473262,37.9558829 C89.1859432,37.9558829 97.0867824,41.0139956 102.787585,46.5723187 C108.470488,52.111019 111.599181,59.7464375 111.599181,68.0649304 C111.599181,78.7425504 107.885984,89.2443007 101.403307,96.8883217 C93.7352545,105.927101 82.6344158,110.77283 69.2938069,110.929278 L68.6249155,110.93319 L64.3638142,110.93319 C62.8816507,110.93319 61.6812902,109.732829 61.6812902,108.250666 C61.6812902,106.831332 62.7841517,105.668751 64.1801338,105.574333 L64.3638142,105.568142 L68.6249155,105.568142 C89.7034951,105.568142 106.234132,91.3135182 106.234132,68.0649304 C106.234132,53.860462 95.665496,43.5292472 80.9931909,43.3240433 L80.5473262,43.3209309 L42.6182238,43.3209309 C41.1371794,43.3209309 39.9356997,42.1200836 39.9356997,40.6384069 C39.9356997,39.2184668 41.039142,38.0564411 42.434616,37.9620708 L42.6182238,37.9558829 L80.5473262,37.9558829 Z M77.2017272,54.5569925 C86.9455413,54.5569925 94.3186718,61.6497377 94.3186718,71.0443285 C94.3186718,77.1775842 92.1634613,82.9077004 88.2454257,87.1766787 C84.0616821,91.7363808 78.3289129,94.2021471 71.6634654,94.3255988 L71.1353194,94.3304742 L29.3434394,94.3304742 C27.8625661,94.3304742 26.6625216,93.1297974 26.6625216,91.6479502 C26.6625216,90.2290828 27.7649361,89.0675488 29.1598957,88.9732176 L29.3434394,88.9670323 L71.1353194,88.9670323 C81.9015616,88.9670323 88.9552299,81.0313689 88.9552299,71.0443285 C88.9552299,64.7002897 84.1999396,60.0808199 77.5474963,59.9260502 L77.2017272,59.9220405 L35.4355455,59.9220405 C33.9546722,59.9220405 32.7546277,58.7213637 32.7546277,57.2395165 C32.7546277,55.8204854 33.8571986,54.6576264 35.2520214,54.5631851 L35.4355455,54.5569925 L77.2017272,54.5569925 Z M25.2638324,54.4879283 C26.7455091,54.4879283 27.9463564,55.6894079 27.9463564,57.1704523 C27.9463564,58.5908589 26.8439419,59.752457 25.4475687,59.8467909 L25.2638324,59.8529763 L15.2190047,59.8529763 C13.7368413,59.8529763 12.5364807,58.6526157 12.5364807,57.1704523 C12.5364807,55.7511181 13.6393422,54.5885369 15.0353243,54.4941193 L15.2190047,54.4879283 L25.2638324,54.4879283 Z"
        />
      </svg>
    </div>
  );
});

interface RestrictedModeProps {
  capabilities: Capabilities;
  onUnrestrict(capabilities: Capabilities): void;
}

const RestrictedMode = React.memo(function RestrictedMode(props: RestrictedModeProps) {
  const { capabilities, onUnrestrict } = props;
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

    case 'coordinator-overlord':
      label = 'Coordinator/Overlord mode';
      message = (
        <p>
          It appears that you are accessing the console on the Coordinator/Overlord shared service.
          Due to the lack of access to some APIs on this service the console will operate in a
          limited mode. The unrestricted version of the console can be accessed on the Router
          service.
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
          access to some APIs on this service the console will operate in a limited mode. The
          unrestricted version of the console can be accessed on the Router service.
        </p>
      );
      break;

    default:
      label = 'Restricted mode';
      message = (
        <p>
          Due to the lack of access to some APIs on this service the console will operate in a
          limited mode. The unrestricted version of the console can be accessed on the Router
          service.
        </p>
      );
      break;
  }

  return (
    <Popover2
      content={
        <PopoverText>
          <p>The console is running in restricted mode.</p>
          {message}
          <p>
            For more info check out the{' '}
            <ExternalLink href={`${getLink('DOCS')}/operations/web-console.html`}>
              web console documentation
            </ExternalLink>
            .
          </p>
          <p>
            It is possible that there is an issue with the capability detection. You can enable the
            unrestricted console but certain features might not work if the underlying APIs are not
            available.
          </p>
          <p>
            <Button
              icon={IconNames.WARNING_SIGN}
              text={`Temporarily unrestrict console${capabilities.hasSql() ? '' : ' (with SQL)'}`}
              onClick={() => onUnrestrict(Capabilities.FULL)}
            />
          </p>
          {!capabilities.hasSql() && (
            <p>
              <Button
                icon={IconNames.WARNING_SIGN}
                text="Temporarily unrestrict console (without SQL)"
                onClick={() => onUnrestrict(Capabilities.NO_SQL)}
              />
            </p>
          )}
        </PopoverText>
      }
      position={Position.BOTTOM_RIGHT}
    >
      <Button icon={IconNames.WARNING_SIGN} text={label} intent={Intent.WARNING} minimal />
    </Popover2>
  );
});

export interface HeaderBarProps {
  active: HeaderActiveTab;
  capabilities: Capabilities;
  onUnrestrict(capabilities: Capabilities): void;
}

export const HeaderBar = React.memo(function HeaderBar(props: HeaderBarProps) {
  const { active, capabilities, onUnrestrict } = props;
  const [aboutDialogOpen, setAboutDialogOpen] = useState(false);
  const [doctorDialogOpen, setDoctorDialogOpen] = useState(false);
  const [coordinatorDynamicConfigDialogOpen, setCoordinatorDynamicConfigDialogOpen] =
    useState(false);
  const [overlordDynamicConfigDialogOpen, setOverlordDynamicConfigDialogOpen] = useState(false);

  const showSplitDataLoaderMenu = capabilities.hasMultiStageQuery();

  const loadDataViewsMenuActive = oneOf(
    active,
    'data-loader',
    'streaming-data-loader',
    'classic-batch-data-loader',
    'sql-data-loader',
  );
  const loadDataViewsMenu = (
    <Menu>
      <MenuItem
        icon={IconNames.FEED}
        text="Streaming"
        href="#streaming-data-loader"
        selected={active === 'streaming-data-loader'}
      />
      <MenuItem
        icon={IconNames.CLEAN}
        text="Batch - SQL"
        href="#sql-data-loader"
        labelElement={<Tag minimal>multi-stage-query</Tag>}
        selected={active === 'sql-data-loader'}
      />
      <MenuItem
        icon={IconNames.LIST}
        text="Batch - classic"
        href="#classic-batch-data-loader"
        selected={active === 'classic-batch-data-loader'}
      />
    </Menu>
  );

  const moreViewsMenuActive = oneOf(active, 'lookups');
  const moreViewsMenu = (
    <Menu>
      <MenuItem
        icon={IconNames.PROPERTIES}
        text="Lookups"
        href="#lookups"
        disabled={!capabilities.hasCoordinatorAccess()}
        selected={active === 'lookups'}
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

  function setForcedMode(capabilities: Capabilities | undefined): void {
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

      <MenuDivider />
      <MenuItem icon={IconNames.COG} text="Console options">
        {capabilitiesOverride ? (
          <MenuItem text="Clear forced mode" onClick={() => setForcedMode(undefined)} />
        ) : (
          <>
            {capabilitiesMode !== 'coordinator-overlord' && (
              <MenuItem
                text="Force Coordinator/Overlord mode"
                onClick={() => setForcedMode(Capabilities.COORDINATOR_OVERLORD)}
              />
            )}
            {capabilitiesMode !== 'coordinator' && (
              <MenuItem
                text="Force Coordinator mode"
                onClick={() => setForcedMode(Capabilities.COORDINATOR)}
              />
            )}
            {capabilitiesMode !== 'overlord' && (
              <MenuItem
                text="Force Overlord mode"
                onClick={() => setForcedMode(Capabilities.OVERLORD)}
              />
            )}
            {capabilitiesMode !== 'no-proxy' && (
              <MenuItem
                text="Force no management proxy mode"
                onClick={() => setForcedMode(Capabilities.NO_PROXY)}
              />
            )}
          </>
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
          active={active === 'workbench'}
          icon={IconNames.APPLICATION}
          text="Query"
          href="#workbench"
          disabled={!capabilities.hasQuerying()}
        />
        {showSplitDataLoaderMenu ? (
          <Popover2
            content={loadDataViewsMenu}
            disabled={!capabilities.hasEverything()}
            position={Position.BOTTOM_LEFT}
          >
            <Button
              className="header-entry"
              icon={IconNames.CLOUD_UPLOAD}
              text="Load data"
              minimal
              active={loadDataViewsMenuActive}
              disabled={!capabilities.hasEverything()}
            />
          </Popover2>
        ) : (
          <AnchorButton
            className="header-entry"
            icon={IconNames.CLOUD_UPLOAD}
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
          active={active === 'datasources'}
          icon={IconNames.MULTI_SELECT}
          text="Datasources"
          href="#datasources"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={active === 'ingestion'}
          icon={IconNames.GANTT_CHART}
          text="Ingestion"
          href="#ingestion"
          disabled={!capabilities.hasSqlOrOverlordAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={active === 'segments'}
          icon={IconNames.STACKED_CHART}
          text="Segments"
          href="#segments"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <AnchorButton
          className="header-entry"
          minimal
          active={active === 'services'}
          icon={IconNames.DATABASE}
          text="Services"
          href="#services"
          disabled={!capabilities.hasSqlOrCoordinatorAccess()}
        />
        <Popover2 content={moreViewsMenu} position={Position.BOTTOM_LEFT}>
          <Button
            className="header-entry"
            minimal
            icon={IconNames.MORE}
            active={moreViewsMenuActive}
          />
        </Popover2>
      </NavbarGroup>
      <NavbarGroup align={Alignment.RIGHT}>
        <RestrictedMode capabilities={capabilities} onUnrestrict={onUnrestrict} />
        <Popover2 content={configMenu} position={Position.BOTTOM_RIGHT}>
          <Button className="header-entry" minimal icon={IconNames.COG} />
        </Popover2>
        <Popover2 content={helpMenu} position={Position.BOTTOM_RIGHT}>
          <Button className="header-entry" minimal icon={IconNames.HELP} />
        </Popover2>
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
