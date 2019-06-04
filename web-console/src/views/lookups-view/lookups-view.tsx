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

import { Button, Icon, Intent, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as classNames from 'classnames';
import * as React from 'react';
import ReactTable from 'react-table';

import { ActionCell, TableColumnSelector, ViewControlBar } from '../../components';
import { AsyncActionDialog, LookupEditDialog } from '../../dialogs/';
import { AppToaster } from '../../singletons/toaster';
import {
  getDruidErrorMessage, LocalStorageKeys,
  QueryManager,
  TableColumnSelectionHandler
} from '../../utils';
import { BasicAction, basicActionsToMenu } from '../../utils/basic-action';

import './lookups-view.scss';


const tableColumns: string[] = ['Lookup name', 'Tier', 'Type', 'Version', ActionCell.COLUMN_LABEL];

const DEFAULT_LOOKUP_TIER: string = '__default';

export interface LookupsViewProps extends React.Props<any> {

}

export interface LookupsViewState {
  lookups: {}[] | null;
  loadingLookups: boolean;
  lookupsError: string | null;
  lookupsUninitialized: boolean;
  lookupEditDialogOpen: boolean;
  lookupEditName: string;
  lookupEditTier: string;
  lookupEditVersion: string;
  lookupEditSpec: string;
  isEdit: boolean;
  allLookupTiers: string[];

  deleteLookupName: string | null;
  deleteLookupTier: string | null;
}

export class LookupsView extends React.Component<LookupsViewProps, LookupsViewState> {
  private lookupsGetQueryManager: QueryManager<string, {lookupEntries: any[], tiers: string[]}>;
  private tableColumnSelectionHandler: TableColumnSelectionHandler;

  constructor(props: LookupsViewProps, context: any) {
    super(props, context);
    this.state = {
      lookups: [],
      loadingLookups: true,
      lookupsError: null,
      lookupsUninitialized: false,
      lookupEditDialogOpen: false,
      lookupEditTier: '',
      lookupEditName: '',
      lookupEditVersion: '',
      lookupEditSpec: '',
      isEdit: false,
      allLookupTiers: [],

      deleteLookupTier: null,
      deleteLookupName: null
    };
    this.tableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.LOOKUP_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  componentDidMount(): void {
    this.lookupsGetQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const tiersResp = await axios.get('/druid/coordinator/v1/lookups/config?discover=true');
        const tiers = tiersResp.data && tiersResp.data.length > 0 ? tiersResp.data : [DEFAULT_LOOKUP_TIER];

        const lookupEntries: {}[] = [];
        const lookupResp = await axios.get('/druid/coordinator/v1/lookups/config/all');
        const lookupData = lookupResp.data;
        Object.keys(lookupData).map((tier: string) => {
          const lookupIds = lookupData[tier];
          Object.keys(lookupIds).map((id: string) => {
            lookupEntries.push({tier, id, version: lookupIds[id].version, spec: lookupIds[id].lookupExtractorFactory});
          });
        });

        return {
          lookupEntries,
          tiers
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          lookups: result ? result.lookupEntries : null,
          loadingLookups: loading,
          lookupsError: error,
          lookupsUninitialized: error === 'Request failed with status code 404',
          allLookupTiers: result === null ? [] : result.tiers
        });
      }
    });

    this.lookupsGetQueryManager.runQuery('dummy');
  }

  componentWillUnmount(): void {
    this.lookupsGetQueryManager.terminate();
  }

  private async initializeLookup() {
    try {
      await axios.post(`/druid/coordinator/v1/lookups/config`, {});
      this.lookupsGetQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show(
        {
          icon: IconNames.ERROR,
          intent: Intent.DANGER,
          message: getDruidErrorMessage(e)
        }
      );
    }
  }

  private async openLookupEditDialog(tier: string, id: string) {
    const { lookups, allLookupTiers } = this.state;
    if (!lookups) return;

    const target: any = lookups.find((lookupEntry: any) => {
      return lookupEntry.tier === tier && lookupEntry.id === id;
    });
    if (id === '') {
      this.setState({
        lookupEditName: '',
        lookupEditTier: allLookupTiers[0],
        lookupEditDialogOpen: true,
        lookupEditSpec: '',
        lookupEditVersion: (new Date()).toISOString(),
        isEdit: false
      });
    } else {
      this.setState({
        lookupEditName: id,
        lookupEditTier: tier,
        lookupEditDialogOpen: true,
        lookupEditSpec: JSON.stringify(target.spec, null, 2),
        lookupEditVersion: target.version,
        isEdit: true
      });
    }
  }

  private changeLookup(field: string, value: string) {
    this.setState({
      [field]: value
    } as any);
  }

  private async submitLookupEdit() {
    const { lookupEditTier, lookupEditName, lookupEditSpec, lookupEditVersion, isEdit } = this.state;
    let endpoint = '/druid/coordinator/v1/lookups/config';
    const specJSON: any = JSON.parse(lookupEditSpec);
    let dataJSON: any;
    if (isEdit) {
      endpoint = `${endpoint}/${lookupEditTier}/${lookupEditName}`;
      dataJSON = {
        version: lookupEditVersion,
        lookupExtractorFactory: specJSON
      };
    } else {
      dataJSON = {
        [lookupEditTier]: {
          [lookupEditName]: {
            version: lookupEditVersion,
            lookupExtractorFactory: specJSON
          }
        }
      };
    }
    try {
      await axios.post(endpoint, dataJSON);
      this.setState({
        lookupEditDialogOpen: false
      });
      this.lookupsGetQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show(
        {
          icon: IconNames.ERROR,
          intent: Intent.DANGER,
          message: getDruidErrorMessage(e)
        }
      );
    }
  }

  private getLookupActions(lookupTier: string, lookupId: string): BasicAction[] {
    return [
      {
        icon: IconNames.EDIT,
        title: 'Edit',
        onAction: () => this.openLookupEditDialog(lookupTier, lookupId)
      },
      {
        icon: IconNames.CROSS,
        title: 'Delete',
        intent: Intent.DANGER,
        onAction: () => this.setState({ deleteLookupTier: lookupTier, deleteLookupName: lookupId })
      }
    ];
  }

  renderDeleteLookupAction() {
    const { deleteLookupTier, deleteLookupName } = this.state;

    return <AsyncActionDialog
      action={
        deleteLookupTier ? async () => {
          await axios.delete(`/druid/coordinator/v1/lookups/config/${deleteLookupTier}/${deleteLookupName}`);
        } : null
      }
      confirmButtonText="Delete lookup"
      successText="Lookup was deleted"
      failText="Could not delete lookup"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ deleteLookupTier: null, deleteLookupName: null });
        if (success) this.lookupsGetQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to delete the lookup '${deleteLookupName}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderLookupsTable() {
    const { lookups, loadingLookups, lookupsError, lookupsUninitialized } = this.state;
    const { tableColumnSelectionHandler } = this;

    if (lookupsUninitialized) {
      return <div className="init-div">
        <Button
          icon={IconNames.BUILD}
          text="Initialize lookup"
          onClick={() => this.initializeLookup()}
        />
      </div>;
    }

    return <>
      <ReactTable
        data={lookups || []}
        loading={loadingLookups}
        noDataText={!loadingLookups && lookups && !lookups.length ? 'No lookups' : (lookupsError || '')}
        filterable
        columns={[
          {
            Header: 'Lookup name',
            id: 'lookup_name',
            accessor: (row: any) => row.id,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn('Lookup name')
          },
          {
            Header: 'Tier',
            id: 'tier',
            accessor: (row: any) => row.tier,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn('Tier')
          },
          {
            Header: 'Type',
            id: 'type',
            accessor: (row: any) => row.spec.type,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn('Type')
          },
          {
            Header: 'Version',
            id: 'version',
            accessor: (row: any) => row.version,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn('Version')
          },
          {
            Header: ActionCell.COLUMN_LABEL,
            id: ActionCell.COLUMN_ID,
            width: ActionCell.COLUMN_WIDTH,
            accessor: row => ({id: row.id, tier: row.tier}),
            filterable: false,
            Cell: (row: any) => {
              const lookupId = row.value.id;
              const lookupTier = row.value.tier;
              const lookupActions = this.getLookupActions(lookupTier, lookupId);
              return <ActionCell actions={lookupActions}/>;
            },
            show: tableColumnSelectionHandler.showColumn(ActionCell.COLUMN_LABEL)
          }
        ]}
        defaultPageSize={50}
      />
    </>;
  }

  renderLookupEditDialog() {
    const { lookupEditDialogOpen, allLookupTiers, lookupEditSpec, lookupEditTier, lookupEditName, lookupEditVersion, isEdit} = this.state;

    return <LookupEditDialog
      isOpen={lookupEditDialogOpen}
      onClose={() => this.setState({ lookupEditDialogOpen: false })}
      onSubmit={() => this.submitLookupEdit()}
      onChange={(field: string, value: string) => this.changeLookup(field, value)}
      lookupSpec={lookupEditSpec}
      lookupName={lookupEditName}
      lookupTier={lookupEditTier}
      lookupVersion={lookupEditVersion}
      isEdit={isEdit}
      allLookupTiers={allLookupTiers}
    />;
  }

  render() {
    const { lookupsError} = this.state;
    const { tableColumnSelectionHandler } = this;

    return<div className="lookups-view app-view">
      <ViewControlBar label="Lookups">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.lookupsGetQueryManager.rerunLastQuery()}
        />
        {
          !lookupsError &&
          <Button
            icon={IconNames.PLUS}
            text="Add"
            onClick={() => this.openLookupEditDialog('', '')}
          />
        }
        <TableColumnSelector
          columns={tableColumns}
          onChange={(column) => tableColumnSelectionHandler.changeTableColumnSelector(column)}
          tableColumnsHidden={tableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderLookupsTable()}
      {this.renderLookupEditDialog()}
      {this.renderDeleteLookupAction()}
    </div>;
  }
}
