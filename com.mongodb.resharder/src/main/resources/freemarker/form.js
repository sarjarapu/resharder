$(function() {  
	$(".button").click(function() {
		  // TODO - validate and process form here
		  var ns = $("input#frmNamespace").val();
		  var targetns = $("input#frmTargetns").val();
		  var readBatch = $("input#frmReadBatch").val();
		  var writeBatch = $("input#frmWriteBatch").val();
		  var host = $("input#frmHost").val();
		  
		  var dataString = "namespace=" + ns + "&targetns=" + targetns + "&readBatch=" + readBatch + "&writeBatch=" + writeBatch + "&host=" + host;
		  
		  $.ajax({  
			  type: "GET",  
			  url: "/reshard",  
			  data: dataString,
			  //data: { "namespace": ns, "targetns": targetns, "readBatch": readBatch, "writeBatch": writeBatch, "host": host }, 
			  success: function() {  
				  $('#formDiv').html("<div id='term' class='console'></div>");  
				  $('#term').html("Resharding initiated for " + ns + "...<br>");  
			  }
		  });  
		  return false;
	  	});
});

