var auto_refresh = setInterval(function() {
	if ($('#term').length > 0) {
		$('.messageLog').load('/getStatus', function(data) {
			if (data.localeCompare("undefined") != 0)
				document.getElementById("term").innerHTML += data
			
			document.getElementById('term').scrollTop = document.getElementById('term').scrollHeight;
				
			if (data.indexOf("10 seconds") > 0) {
				// TODO collections are synced so make the stop button visible
			}
		});
		
		$('.counterClass').load('/getCounters', function(data) {
			document.getElementById("monitor").innerHTML = data
		});
		
		$('.synchClass').load('/isActive', function(data) {
			document.getElementById("synch").innerHTML = data;
			
			if (data.localeCompare("false") == 0) {
				$('#synchDiv').html("OpLog replay is synchronized.  Disconnect data writers and then hit STOP.<br><button type='submit' name='submit' class='stopButton' id='stop_btn' value='Submit'>STOP</button><br>");
				$('.stopButton').click(function() {
					$.ajax({
						type : "POST",
						url : "/end",
						success : function() {
							$('#synchDiv').hide();
							clearInterval(auto_refresh);
						}
					});	
				});
			}
		});
		
		$('.counterVals').load('/getChartData'), function(data) {
			document.getElementById("counterVals").innerHTML = data;
		}
	}
}, 1000); // refresh every second

$(function() {
	
	$(".button").click(function() {
						// TODO - validate and process form here
						var ns = $("input#frmNamespace").val();
						var targetns = $("input#frmTargetns").val();
						var readBatch = $("input#frmReadBatch").val();
						var writeBatch = $("input#frmWriteBatch").val();
						var srchost = $("input#frmSrc").val();
						var tgthost = $("input#frmTgt").val();
						var loghost = $("input#frmLog").val();
						var reshard = $("input#frmReshard").val();
						var key = $("input#frmKey").val();
						var secondary = $("input#cbxSecondary").val();

						$('#formDiv').html("<div id='container' style='min-width: 310px; height: 300px; margin: 0 auto'></div><br><div id='synchDiv'></div><br>");
						
						$('#perfTitle').html("Clone/Reshard Progress");
						$('#perfGraph').html("<div id='term' class='console'></div>");
						$('#perfCounters').html("<div id='monitor' class='consoleSmall'</div><br>");
						

						var dataString = "namespace=" + ns + "&targetns="
								+ targetns + "&readBatch=" + readBatch
								+ "&writeBatch=" + writeBatch + "&srchost="
								+ srchost + "&tgthost=" + tgthost + "&loghost="
								+ loghost + "&reshard=" + reshard + "&key="
								+ key + "&secondary=" + secondary;

						$.ajax({
							type : "GET",
							url : "/reshard",
							data : dataString,
							success : function() {
								$('#term').html("Resharding initiated for " + ns + "...<br>");
							}
						});

						$(function () {
						    $(document).ready(function() {
						        Highcharts.setOptions({
						            global: {
						                useUTC: false
						            }
						        });
						    
						        var chart;
						        $('#container').highcharts({
						            chart: {
						                type: 'spline',
						                animation: Highcharts.svg, // don't
																	// animate
																	// in old IE
						                marginRight: 10,
						                events: {
						                    load: function() {
						    
						                        // set up the updating of the
												// chart each second
												var docsPerSec = this.series[0];
												var oplogPerSec = this.series[1];
												var orphansPerSec = this.series[2];
												var oplogCopiesPerSec = this.series[3];
												
						                        setInterval(function() {
															var x = (new Date()).getTime(), 
															y = Math.random();
														
															counters = JSON.parse(document.getElementById("counterVals").innerHTML);
															
															docsPerSec.addPoint([x, counters.docs ], true, true);
															oplogPerSec.addPoint([x, counters.oplogs ], true, true);
															orphansPerSec.addPoint([x, counters.orphans ], true, true);
															oplogCopiesPerSec.addPoint([x, counters.oplogCopies ], true, true);
														}, 1000);
						                    }
						                }
						            },
						            title: {
						                text: 'Processing Rates'
						            },
						            xAxis: {
						                type: 'datetime',
						                tickPixelInterval: 150
						            },
						            yAxis: {
						                title: {
						                    text: 'Value'
						                },
						                plotLines: [{
						                    value: 0,
						                    width: 1,
						                    color: '#808080'
						                }]
						            },
						            tooltip: {
						                formatter: function() {
						                        return '<b>'+ this.series.name +'</b><br/>'+
						                        Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) +'<br/>'+
						                        Highcharts.numberFormat(this.y, 2);
						                }
						            },
						            legend: {
						                enabled: false
						            },
						            exporting: {
						                enabled: false
						            },
						            series: [ {
										name : 'Documents/sec',
										data : (function() {
											var data = [], time = (new Date()).getTime(), i;

											for (i = -19; i <= 0; i++) {
												data.push({ x : time + i * 1000, y : 0 });
											}
											return data;
										})()
									},{
										name : 'Oplogs/sec',
										data : (function() {
											var data = [], time = (new Date()).getTime(), i;

											for (i = -19; i <= 0; i++) {
												data.push({ x : time + i * 1000, y : 0 });
											}
											return data;
										})()
									},{
										name : 'Orphans/sec',
										data : (function() {
											var data = [], time = (new Date()).getTime(), i;

											for (i = -19; i <= 0; i++) {
												data.push({ x : time + i * 1000, y : 0 });
											}
											return data;
										})()
									},{
										name : 'Oplog Copies/sec',
										data : (function() {
											var data = [], time = (new Date()).getTime(), i;

											for (i = -19; i <= 0; i++) {
												data.push({ x : time + i * 1000, y : 0 });
											}
											return data;
										})()
									} ]
						        });
						    });
						    
						});

						return false;
					});
});
