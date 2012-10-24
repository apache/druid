var DruidTable = function() {
  var _private = {
    data: []
  };

  var _public = {
    getRows: function() {
      return _private.data;
    },

    getRow: function(row) {
      return _private.data[row];
    },

    getNumRows: function() {
      return _private.data.length;
    },

    getCol: function(colName) {
      return _.map(_private.data, function(dataRow) {
        return dataRow[colName];
      });
    },

    getColTotal: function(colName) {
      var total = 0;

      _.each(this.getCol(colName), function(num) {
        total += 0 + num;
      });
      return total;
    },

    getCell: function(row, col) {
      return _private.data[row][col];
    },

    setCell: function(row, col, val) {
      _private.data[row] = _private.data[row] || {};
      _private.data[row][col] = val;
    },

    addColumn: function(colName, colVals) {
      var i = 0;

      _.each(_private.data, function(row) {
        row[colName] = colVals[i];
        i++;
      })
    },

    toHTMLTable: function(el) {
      var html = "";

      // build table header
      html += "<thead>";

      // build table header filters
      html += "<tr>";
      for (var field in this.getRow(0)) {
        html += "<td><input type = \"text\" name=\"" + field + "\" value=\"" + field + "\" class=\"search_init\"/></td>";
      }
      html += "</tr>";

      // build table header column headings
      html += "<tr>";
      for (var field in this.getRow(0)) {
        html += "<th>" + field + "</th>";
      }
      html += "</tr>";
      html += "</thead>";

      // build table body
      html += "<tbody>";
      for (var r in this.getRows()) {
        html += "<tr>";
        for (var field in this.getRow(r)) {
          html += "<td " + "class=\"" + field.replace(' ', '_').toLowerCase() + "\">" + this.getCell(r, field) + "</td>";
        }
        html += "</tr>";
      }
      html += "</tbody>";

      el.html(html);
    }
  };

  return _public;
};