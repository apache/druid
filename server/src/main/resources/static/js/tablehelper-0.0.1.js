// requires dataTables

var oTable = null;

// flattens JSON from Druid and builds a table row per segment
function initTables(data) {
  var resultTable = new DruidTable();
  var row = 0;

  // parse JSON
  for (var entry in data) {
    // build server table
    for (var field in data[entry]) {
      if (!(data[entry][field] instanceof Object)) {
        resultTable.setCell(row, field, data[entry][field]);
      }
    }
    row++;
  }

  resultTable.toHTMLTable($('#result_table'));
}

function initDataTable(el) {
  // dataTable stuff (http://www.datatables.net/)
  var asInitVals = [];

  oTable = el.dataTable({
    "oLanguage": {
      "sSearch": "Search all columns:"
    },
    "oSearch": {
      "sSearch": "",
      "bRegex": true
    },
    "sPaginationType": "full_numbers",
    "bProcessing": true,
    "bDeferRender": true
  });


  $("thead input").keyup(function() {
      oTable.fnFilter(this.value, oTable.children("thead").find("input").index(this), true);
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

function buildTable(data, el) {
  if (oTable != null) {
    oTable.fnDestroy();
    el.empty();
  }

  initTables(data);
  initDataTable(el);
}