// requires tableHelper

var oTable = [];

$(document).ready(function() {
  var augment = function(data) {
    for (i = 0 ; i < data.length ; i++) {
      var taskId = encodeURIComponent(data[i].id)
      data[i].more =
        '<a href="/druid/indexer/v1/task/' + taskId + '">payload</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/status">status</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log">log (all)</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log?offset=-8192">log (last 8kb)</a>'
    }
  }

  $.get('/druid/indexer/v1/runningTasks', function(data) {
    $('.running_loading').hide();
    augment(data);
    buildTable(data, $('#runningTable'));
  });

  $.get('/druid/indexer/v1/pendingTasks', function(data) {
    $('.pending_loading').hide();
    augment(data);
    buildTable(data, $('#pendingTable'));
  });

  $.get('/druid/indexer/v1/waitingTasks', function(data) {
    $('.waiting_loading').hide();
    augment(data);
    buildTable(data, $('#waitingTable'));
  });

  $.get('/druid/indexer/v1/completeTasks', function(data) {
    $('.complete_loading').hide();
    augment(data);
    buildTable(data, $('#completeTable'));
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
