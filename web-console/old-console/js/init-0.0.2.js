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

var CONSOLE = {};
CONSOLE.selected_segments = [];

$(document).ready(function() {
  // flattens JSON from Druid and builds a table row per segment
  function initTables(data) {
    var serverTable = new DruidTable(),
        segmentTable = new DruidTable(),
        serverRow = 0,
        segmentRow = 0;

    // parse JSON
    for (var server in data) {
      var segments = {};

      // build server table
      for (var field in data[server]) {
        if (!(data[server][field] instanceof Object)) {
          serverTable.setCell(serverRow, 'Server ' + field, data[server][field]);
        } else {
          segments = data[server][field];
        }
      }
      serverRow++;

      // build segment table
      for (var segment in segments) {
        segmentTable.setCell(segmentRow, 'Server host', data[server]['host']);

        for (field in segments[segment]) {
          if (!(segments[segment][field] instanceof Object)) {
            segmentTable.setCell(segmentRow, 'Segment ' + field, segments[segment][field]);
          }
        }
        segmentRow++;
      }
    }

    serverTable.addColumn('Server percentUsed', function() {
      var ret = [];

      var currSizes = serverTable.getCol('Server currSize');
      var maxSizes = serverTable.getCol('Server maxSize');

      for (var i = 0; i < currSizes.length; i++) {
        if (maxSizes[i] === 0) {
          ret.push(0);
        } else {
          ret.push(100*(currSizes[i]/maxSizes[i]));
        }
      }

      return ret;
    }());
    var avg = serverTable.getColTotal('Server percentUsed') / serverTable.getNumRows();
    if (!isNaN(avg)) {
      $('#avg_server_metric').html('Average Server Percent Used: ' + avg + '%');
    }else{
      $('.loading').html('Server is still starting...Please try after few minutes.');
      $('.loading').show();
    }
    serverTable.toHTMLTable($('#servers'));
    segmentTable.toHTMLTable($('#segments'));
  }

  function initDataTable(el, oTable) {
    // dataTable stuff (http://www.datatables.net/)
    var asInitVals = [];

    oTable[el.attr('id')] = el.dataTable({
      "oLanguage": {
        "sSearch": "Search all columns:"
      },
      "oSearch": {
        "sSearch": "",
        "bRegex": true
      },
      "sPaginationType": "full_numbers",
      "bProcessing": true
    });

    $("thead input").keyup(function() {
        var tbl = oTable[$(this).parents('table').attr('id')];
        tbl.fnFilter(this.value, tbl.children("thead").find("input").index(this), true);
    });

    $("thead input").each(function(i) {
      asInitVals[i] = this.value;
    });

    $("thead input").focus(function() {
      if (this.className === "search_init" ) {
        this.className = "";
        this.value = "";
      }
    });

    $("thead input").blur(function(i) {
      if (this.value === "" ) {
        this.className = "search_init";
        this.value = asInitVals[$("thead input").index(this)];
      }
    });
  }

  // Execution stuff
  $.get('/druid/coordinator/v1/leader', function(data) {
    $("#coordinator").html('Current Cluster Coordinator Leader: ' + data.host);
  });

  $('#move_segment').submit(function() {
    var data = [];

    if ($.isEmptyObject(CONSOLE.selected_segments)) {
      alert("Please select at least one segment");
    }
    for (var seg in CONSOLE.selected_segments) {
      data.push({
        'segmentName' : seg,
        'from' : CONSOLE.selected_segments[seg].children('.server_host').text(),
        'to' : $('#move_segment > .to').val()
      });
    }

    return false;
  });

  $.get('/druid/coordinator/v1/servers?full', function(data) {
    $('.loading').hide();

    initTables(data);

    var oTable = [];
    initDataTable($('#servers'), oTable);
    initDataTable($('#segments'), oTable);
  });
});
