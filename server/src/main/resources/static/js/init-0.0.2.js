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
        segmentTable.setCell(segmentRow, 'Segment name', segment);

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
    $('#avg_server_metric').html('Average Server Percent Used: ' + avg + '%');

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
  $.get('/info/master', function(data) {
    $("#master").html('Current Cluster Master: ' + data.host);
  });

  $('#move_segment').submit(function() {
    var data = [];

    if ($.isEmptyObject(CONSOLE.selected_segments)) {
      alert("Please select at least one segment");
    }
    for (seg in CONSOLE.selected_segments) {
      data.push({
        'segmentName' : seg,
        'from' : CONSOLE.selected_segments[seg].children('.server_host').text(),
        'to' : $('#move_segment > .to').val()
      });
    }

    $.ajax({
      url:"/master/move",
      type: "POST",
      data: JSON.stringify(data),
      contentType:"application/json; charset=utf-8",
      dataType:"json",
      error: function(xhr, status, error) {
        alert(error + ": " + xhr.responseText);
      },
      success: function(data, status, xhr) {
        for (seg in CONSOLE.selected_segments) {
          CONSOLE.selected_segments[seg].children('.server_host').text($('#move_segment > .to').val());
        }
      }
    });
    
    return false;
  });

  /*$('#drop_segment').submit(function() {
    var data = [];

    if ($.isEmptyObject(CONSOLE.selected_segments)) {
      alert("Please select at least one segment");
    }
    for (seg in CONSOLE.selected_segments) {
      data.push({
        'segmentName' : seg,
        'from' : CONSOLE.selected_segments[seg]
      });
    }

    $.ajax({
      url:"/master/drop",
      type: "POST",
      data: JSON.stringify(data),
      contentType:"application/json; charset=utf-8",
      dataType:"json",
      error: function(xhr, status, error) {
        alert(error + ": " + xhr.responseText);
      }
    });

    return false;
  });*/

  $.get('/info/cluster', function(data) {
    $('.loading').hide();
    
    initTables(data);

    var oTable = [];
    initDataTable($('#servers'), oTable);
    initDataTable($('#segments'), oTable);

    // init select segments
    $("#segments tbody").click(function(event) {
      var el = $(event.target.parentNode);
      var key = el.children('.segment_name').text();
      if (el.is("tr")) {
        if (el.hasClass('row_selected')) {
          el.removeClass('row_selected');
          delete CONSOLE.selected_segments[key];
        } else {
          el.addClass('row_selected');
          CONSOLE.selected_segments[key] = el;
        }

        var html ="";
        for (segment in CONSOLE.selected_segments) {
          html += segment + ' on ' + CONSOLE.selected_segments[segment].children('.server_host').text() + '<br/>';
        }
        $('#selected_segments').html(html);
      }
    });
  });
});