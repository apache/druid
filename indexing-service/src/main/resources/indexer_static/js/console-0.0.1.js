// requires tableHelper

var oTable = [];

var killTask = function(taskId) {
  if(confirm('Do you really want to kill: '+taskId)) {
    $.ajax({
      type:'POST',
      url: '/druid/indexer/v1/task/'+ taskId +'/shutdown',
      data: ''
    }).done(function(data) {
      setTimeout(function() { location.reload(true) }, 750);
    }).fail(function(data) {
      alert('Kill request failed with status: '+data.status+' please check overlord logs');
    })
  }
}

var resetSupervisor = function(supervisorId) {
  if(confirm('Do you really want to reset: '+ supervisorId)) {
    $.ajax({
      type:'POST',
      url: '/druid/indexer/v1/supervisor/' + supervisorId + '/reset',
      data: ''
    }).done(function(data) {
      setTimeout(function() { location.reload(true) }, 750);
    }).fail(function(data) {
      alert('Reset request failed, please check overlord logs for details.');
    })
  }
}

var shutdownSupervisor = function(supervisorId) {
  if(confirm('Do you really want to shutdown: '+ supervisorId)) {
    $.ajax({
      type:'POST',
      url: '/druid/indexer/v1/supervisor/' + supervisorId + '/shutdown',
      data: ''
    }).done(function(data) {
      setTimeout(function() { location.reload(true) }, 750);
    }).fail(function(data) {
      alert('Shutdown request failed, please check overlord logs for details.');
    })
  }
}

$(document).ready(function() {
  var augment = function(data, showKill) {
    for (i = 0 ; i < data.length ; i++) {
      var taskId = encodeURIComponent(data[i].id)
      data[i].more =
        '<a href="/druid/indexer/v1/task/' + taskId + '">payload</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/status">status</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log">log (all)</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log?offset=-8192">log (last 8kb)</a>';
      if(showKill) {
        data[i].more += '<a onclick="killTask(\''+ taskId +'\');">kill</a>';
      }
    }
  }

  $.get('/druid/indexer/v1/supervisor', function(dataList) {
    var data = []
    for (i = 0 ; i < dataList.length ; i++) {
      var supervisorId = encodeURIComponent(dataList[i])
      data[i] = {
        "dataSource" : supervisorId,
        "more" :
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '">payload</a>' +
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '/status">status</a>' +
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '/history">history</a>' +
          '<a onclick="resetSupervisor(\'' + supervisorId + '\');">reset</a>' +
          '<a onclick="shutdownSupervisor(\'' + supervisorId + '\');">shutdown</a>'
      }
    }
    buildTable((data), $('#supervisorsTable'));
    if (dataList.length > 0) {
      $('.supervisors_section').show();
    }
  });

  $.get('/druid/indexer/v1/runningTasks', function(data) {
    $('.running_loading').hide();
    augment(data, true);
    buildTable(data, $('#runningTable'));
  });

  $.get('/druid/indexer/v1/pendingTasks', function(data) {
    $('.pending_loading').hide();
    augment(data, true);
    buildTable(data, $('#pendingTable'));
  });

  $.get('/druid/indexer/v1/waitingTasks', function(data) {
    $('.waiting_loading').hide();
    augment(data, true);
    buildTable(data, $('#waitingTable'));
  });

  $.get('/druid/indexer/v1/completeTasks', function(data) {
    $('.complete_loading').hide();
    augment(data, false);
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
