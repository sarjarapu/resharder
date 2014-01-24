var auto_refresh = setInterval(function() {
	if ($('#term').length > 0) {
		$('.messageLog').load('/getStatus', function(data) {
			if (data.localeCompare("undefined") != 0)
				document.getElementById("term").innerHTML += data
			
			document.getElementById('term').scrollTop = document.getElementById('term').scrollHeight;
		});
		
		$('.counterClass').load('/getCounters', function(data) {
			document.getElementById("monitor").innerHTML = data
		});
		
		$('.synchClass').load('/isActive', function(data) {
			document.getElementById("synch").innerHTML = data;

			if (data.localeCompare("done") == 0) {
				setTimeout(function(){clearInterval(auto_refresh);},30000);
			}
			
			if (data.localeCompare("false") == 0 && !shutdown) {
				$( "#dialog-message" ).dialog( "open" );
				shutdown=true;
			}
		});
		
		$('.counterVals').load('/getChartData'), function(data) {
			document.getElementById("counterVals").innerHTML = data;
		}
		
		$('.graphHTML').load('/getGraphHtml'), function(data) {
			document.getElementById("graphHTML").innerHTML = data;
		}
		
		$('.connectionHTML').load('/getConnectionHtml'), function(data) {
			document.getElementById("connectionHTML").innerHTML = data;
		}
		
		drawGraph();
	}
}, 1000); // refresh every second

var shutdown=false;

$("#dialog-message").dialog({
	autoOpen: false,
    draggable: false,
    resizable: false,
    position: ['center', 'center'],
    show: 'blind',
    hide: 'blind',
    width: 400,
    dialogClass: 'ui-dialog-osx',
    buttons: {
        "STOP": function() {$.ajax({
			type : "POST",
			url : "/end",
			success : function() {
				$("#dialog-message").hide();
				$('#container').hide();
	            $("#dialog-message").dialog("close");
			}
		});	
        }
    }
});

var drawGraph = function() {
		// suspend drawing and initialise.
		instance.doWhileSuspended(function() {
										
			instance.detachEveryConnection();
			
			// and finally, make a couple of connections
	        var json = $("#connections").html();
	        json = JSON.parse(json);
	        json = json.connections;
	        for(var entry in json) {
	        	var temp = {source:json[entry].source, target:json[entry].target, parameters:{label:json[entry].label}};
				instance.connect(temp);
	        }
		});
}

var instance;
var windows;

;(function() {
	
	jsPlumb.ready(function() {
	// setup some defaults for jsPlumb.	
			instance = jsPlumb.getInstance({
			Endpoint : ["Dot", {radius:2}],
			HoverPaintStyle : {strokeStyle:"#1e8151", lineWidth:2 },
			ConnectionOverlays : [
				[ "Arrow", { 
					location:1,
					id:"arrow",
                    length:10,
                    width:10,
                    foldback:0.8
				} ],
                [ "Label", { label:"FOO", id:"label", cssClass:"aLabel" }]
			],
			Container:"statemachine-demo"
		});

		windows = instance.getSelector(".statemachine-demo .w");

	    // initialise draggable elements.  
		instance.draggable(windows);

		// bind a connection listener. note that the parameter passed to this function contains more than
		// just the new connection - see the documentation for a full list of what is included in 'info'.
		// this listener sets the connection's internal
		// id as the label overlay's text.
        instance.bind("connection", function(info) {
        		info.connection.getOverlay("label").setLabel(info.connection.getParameter("label"));
        });
        
		// make each ".ep" div a source and give it some parameters to work with.  here we tell it
		// to use a Continuous anchor and the StateMachine connectors, and also we give it the
		// connector's paint style.  note that in this demo the strokeStyle is dynamically generated,
		// which prevents us from just setting a jsPlumb.Defaults.PaintStyle.  but that is what i
		// would recommend you do. Note also here that we use the 'filter' option to tell jsPlumb
		// which parts of the element should actually respond to a drag start.
		instance.makeSource(windows, {
			anchor:"Continuous",
			connector:[ "StateMachine", { curviness:20 } ],
			connectorStyle:{ strokeStyle:"#5c96bc", lineWidth:1, outlineColor:"transparent", outlineWidth:2 },
			maxConnections:500,
			onMaxConnections:function(info, e) {
				alert("Maximum connections (" + info.maxConnections + ") reached");
			}
		});						

		// initialise all '.w' elements as connection targets.
        instance.makeTarget(windows, {
			dropOptions:{ hoverClass:"dragHover" },
			anchor:"Continuous"				
		});
        
		drawGraph();
	});
})();

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
						
						var msg = "WARNING - You are about to initate a cloning operation that ";
						if (secondary.localeCompare("true") == 0) {
							msg += "will cause a disruption in connectivity.  You should close all connections to " + ns + " and restart all mongos instances once document migration starts.  The clone operation ";
						}
						msg += " will place significant load on both the source and target infrastructure.  Confirm?";
						if (confirm(msg) == false)
							return;
						
						$("#formDiv").html("");

						$('#perfGraph').html("<div id='term' class='console' style='position:absolute; top:30px; left:50px; min-width:750px;'></div>" + 
								"<div id='monitor' class='consoleSmall' style='position:absolute; top:430px; left:50px; min-width:750px;'></div>" + 
								"<div id='container' style='position:absolute; top:580px; left:50px; min-width: 750px; height: 300px; margin: 0 auto'></div>");
						

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
