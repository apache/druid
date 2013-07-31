  var grid;
  var columns = [
  {id: "millisToWaitBeforeDeleting", name: "millisToWaitBeforeDeleting", field: "millisToWaitBeforeDeleting",editor: Slick.Editors.LongText},
  {id: "mergeBytesLimit", name: "mergeBytesLimit", field: "mergeBytesLimit",editor: Slick.Editors.LongText},
  {id: "mergeSegmentsLimit", name: "mergeSegmentsLimit", field: "mergeSegmentsLimit",editor: Slick.Editors.LongText},
  {id: "maxSegmentsToMove", name: "maxSegmentsToMove", field: "maxSegmentsToMove",editor: Slick.Editors.LongText}
  ];

  var options = {
  enableCellNavigation: true,
  enableColumnReorder: false,
  editable: true,
  autoEdit: false,
  fullWidthRows: true
  };

  $(function () {
      $.get('../info/master/dynamicConfigs', function (data) {
          var rowData = [];
          rowData[0]=data;
          grid = new Slick.Grid("#myGrid", rowData, columns, options);
          grid.onCellChange.subscribe(function (e) {
              var paramJson = JSON.stringify(grid.getData()[0]);
              $.ajax({
              url:'../info/master/dynamicConfigs',
              type:"POST",
              data:paramJson,
              contentType:"application/json; charset=utf-8",
              dataType:"json"
              });
          });
      });
  });