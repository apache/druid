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

import { Button, Intent } from "@blueprintjs/core";
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as classNames from 'classnames';
import * as React from 'react';
import ReactTable from "react-table";
import { Filter } from "react-table";

import { TableColumnSelection } from "../components/table-column-selection";
import { LookupEditDialog } from "../dialogs/lookup-edit-dialog";
import { AppToaster } from "../singletons/toaster";
import {
  getDruidErrorMessage, LocalStorageKeys,
  QueryManager,
  TableColumnSelectionHandler
} from "../utils";

import "./lookups-view.scss";

const tableColumns: string[] = ["Lookup Name", "Tier", "Type", "Version", "Config"];

export interface LookupsViewProps extends React.Props<any> {

}

export interface LookupsViewState {
  lookups: {}[];
  loadingLookups: boolean;
  lookupsError: string | null;
  lookupEditDialogOpen: boolean;
  lookupEditName: string;
  lookupEditTier: string;
  lookupEditVersion: string;
  lookupEditSpec: string;
  isEdit: boolean;
  allLookupTiers: string[];
}

export class LookupsView extends React.Component<LookupsViewProps, LookupsViewState> {
  private lookupsGetQueryManager: QueryManager<string, {lookupEntries: any[], tiers: string[]}>;
  private lookupDeleteQueryManager: QueryManager<string, any[]>;
  private tableColumnSelectionHandler: TableColumnSelectionHandler;

  constructor(props: LookupsViewProps, context: any) {
    super(props, context);
    this.state = {
      lookups: [],
      loadingLookups: true,
      lookupsError: null,
      lookupEditDialogOpen: false,
      lookupEditTier: "",
      lookupEditName: "",
      lookupEditVersion: "",
      lookupEditSpec: "",
      isEdit: false,
      allLookupTiers: []
    };
    this.tableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.LOOKUP_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  componentDidMount(): void {
    this.lookupsGetQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
        const tiers = tiersResp.data;

        const lookupEntries: {}[] = [];
        const lookupResp = await axios.get("/druid/coordinator/v1/lookups/config/all");
        const lookupData = lookupResp.data;
        Object.keys(lookupData).map((tier: string) => {
          const lookupIds = lookupData[tier];
          Object.keys(lookupIds).map((id: string) => {
            lookupEntries.push({tier, id, version: lookupData[tier][id].version, spec: lookupData[tier][id].lookupExtractorFactory});
          });
        });
        return {
          lookupEntries,
          tiers
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          lookups: result === null ? [] : result.lookupEntries,
          loadingLookups: loading,
          lookupsError: error,
          allLookupTiers: result === null ? [] : result.tiers
        });
      }
    });

    this.lookupsGetQueryManager.runQuery("dummy");

    this.lookupDeleteQueryManager = new QueryManager({
      processQuery: async (url: string) => {
        const lookupDeleteResp = await axios.delete(url);
        return lookupDeleteResp.data;
      },
      onStateChange: ({}) => {
        this.lookupsGetQueryManager.rerunLastQuery();
      }
    });
  }

  componentWillUnmount(): void {
    this.lookupsGetQueryManager.terminate();
    this.lookupDeleteQueryManager.terminate();
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
    const target: any = lookups.find((lookupEntry: any) => {
      return lookupEntry.tier === tier && lookupEntry.id === id;
    });
    if (id === "") {
      this.setState({
        lookupEditName: "",
        lookupEditTier: allLookupTiers[0],
        lookupEditDialogOpen: true,
        lookupEditSpec: "",
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
    let endpoint = "/druid/coordinator/v1/lookups/config";
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

  private deleteLookup(tier: string, name: string): void {
    const url = `/druid/coordinator/v1/lookups/config/${tier}/${name}`;
    this.lookupDeleteQueryManager.runQuery(url);
  }

  renderLookupsTable() {
    const { lookups, loadingLookups, lookupsError } = this.state;
    const { tableColumnSelectionHandler } = this;

    if (lookupsError) {
      return <div className={"init-div"}>
        <Button
          icon={IconNames.BUILD}
          text="Initialize lookup"
          onClick={() => this.initializeLookup()}
        />
      </div>;
    }
    return <>
      <ReactTable
        data={lookups}
        loading={loadingLookups}
        noDataText={!loadingLookups && lookups && !lookups.length ? 'No lookups' : (lookupsError || '')}
        filterable
        columns={[
          {
            Header: "Lookup Name",
            id: "lookup_name",
            accessor: (row: any) => row.id,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn("Lookup Name")
          },
          {
            Header: "Tier",
            id: "tier",
            accessor: (row: any) => row.tier,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn("Tier")
          },
          {
            Header: "Type",
            id: "type",
            accessor: (row: any) => row.spec.type,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn("Type")
          },
          {
            Header: "Version",
            id: "version",
            accessor: (row: any) => row.version,
            filterable: true,
            show: tableColumnSelectionHandler.showColumn("Version")
          },
          {
            Header: "Config",
            id: "config",
            accessor: row => ({id: row.id, tier: row.tier}),
            filterable: false,
            Cell: (row: any) => {
              const lookupId = row.value.id;
              const lookupTier = row.value.tier;
              return <div>
                <a onClick={() => this.openLookupEditDialog(lookupTier, lookupId)}>Edit</a>
                &nbsp;&nbsp;&nbsp;
                <a onClick={() => this.deleteLookup(lookupTier, lookupId)}>Delete</a>
              </div>;
            },
            show: tableColumnSelectionHandler.showColumn("Config")
          }
        ]}
        defaultPageSize={50}
        className="-striped -highlight"
      />
    </>;
  }

  renderLookupEditDialog() {
    const { lookupEditDialogOpen, allLookupTiers, lookupEditSpec, lookupEditTier, lookupEditName, lookupEditVersion, isEdit } = this.state;

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
    const { tableColumnSelectionHandler } = this;
    return <div className="lookups-view app-view">
      <div className="control-bar">
        <div className="control-label">Lookups</div>
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.lookupsGetQueryManager.rerunLastQuery()}
        />
        <Button
          icon={IconNames.PLUS}
          text="Add"
          style={{display: this.state.lookupsError !== null ? 'none' : 'inline'}}
          onClick={() => this.openLookupEditDialog("", "")}
        />
        <TableColumnSelection
          columns={tableColumns}
          onChange={(column) => tableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={tableColumnSelectionHandler.hiddenColumns}
        />
      </div>
      {this.renderLookupsTable()}
      {this.renderLookupEditDialog()}
    </div>;
  }
}
