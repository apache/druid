// requires tableHelper

var oTable = [];

$(document).ready(function() {
  $.get('/druid/indexer/v1/runningTasks', function(data) {
    $('.running_loading').hide();
    buildTable(data, $('#runningTable'), ["segments"]);
  });

  $.get('/druid/indexer/v1/pendingTasks', function(data) {
    $('.pending_loading').hide();
    buildTable(data, $('#pendingTable'), ["segments"]);
  });

  $.get('/druid/indexer/v1/workers', function(data) {
    $('.workers_loading').hide();
    buildTable(data, $('#workerTable'));
  });

  $.get('/druid/indexer/v1/scaling', function(data) {
    $('.events_loading').hide();
    buildTable(data, $('#eventTable'));
  });
});