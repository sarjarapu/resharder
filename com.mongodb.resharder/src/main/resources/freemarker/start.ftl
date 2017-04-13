<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html><head>

  
  <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type"><title>MongoDB Resharder</title>
  


  
  <link rel="stylesheet" href="graph.css">
  <link rel="stylesheet" href="graph1.css">
  <link rel="stylesheet" type="text/css" href="jquery-ui-1.10.4.custom.min.css">

  
  <style type="text/css">
  .ui-dialog-osx {
    -moz-border-radius: 0 0 8px 8px;
    -webkit-border-radius: 0 0 8px 8px;
    border-radius: 0 0 8px 8px; border-width: 0 8px 8px 8px;
    font: normal normal normal 10px/1.5 Arial, Helvetica, sans-serif;
}

.console {
  font-family:Courier;
 color: #CCCCCC;
  background: #000000;
  border: 3px double #CCCCCC;
  padding: 10px;
  overflow: scroll;
  text-align: left;
  font-size: 75%;
  height: 400px;
}


.consoleSmall {
  font-family:Courier;
 color: #CCCCCC;
  background: #000000;
  border: 3px double #CCCCCC;
  padding: 10px;
  overflow: scroll;
  text-align: left;
  font-size: 75%;
  height: 100px;
}
  </style></head><body>
<div id="messages" class="messageLog" style="display: none;"></div>
<div id="graphUpdate" class="graphUpdate" style="display: none;">true</div>
<div id="counters" class="counterClass" style="display: none;"></div>
<div id="synch" class="synchClass" style="display: none;"></div>
<div id="counterVals" class="counterVals" style="display: none;">${data}</div>
<div id="connectionHTML" class="connectionHTML" style="display: none;">${connectionHTML}</div>

<div id="title" style="text-align: center; min-width: 1400px;"><big><big><big>MongoDB
Resharder</big></big></big></div>

<div style="min-width: 1600px;">
<div id="perfGraph" name="perfGraph" style="position: absolute; top: 70px; left: 50px; min-width: 800px;">
<pre style="overflow: auto; margin-top: 13px; height: 800px; width: 800px;" id="json" class="prettyprint">${collections}</pre>
<br class="clear">
</div>
<div id="main" style="position: absolute; top: 0px; left: 900px; width: 600px; height: 800px;">
<div class="explanation">
<h4>WORKFLOW STATE</h4>
</div>
<div class="demo statemachine-demo" id="statemachine-demo">
<div id="graphHTML" class="graphHTML"> ${graphHTML} </div>
</div>
</div>
</div>

<div id="formDiv" style="position: absolute; top: 700px; left: 800px; width: 600px;">
<form name="reshard" action="" style="position: relative; left: 10em; text-align: center;">
  <table style="width: 512px; height: 294px;">
    <tbody>
      <tr>
        <td colspan="5" rowspan="1">Resharder Options<br>
</td>
      </tr>
      <tr>
        <td style="text-align: center;">Select
Collection<br>
        </td>
        <td style="text-align: right;">Source Host<br>
        </td>
        <td colspan="3" rowspan="1"><input style="width: 250px;" id="frmSrc" name="srchost" value="localhost:28010"><br>
        </td>
      </tr>
      <tr>
        <td colspan="1" rowspan="8" style="vertical-align: top; text-align: left;"> 
<#list collList as coll>
        <p><input name="namespace" id="frmNamespace" value="${coll}" type="radio">${coll} </p>
</#list>
        </td>
        <td style="vertical-align: middle; text-align: right;">Target
Host<br>
        </td>
        <td colspan="3" rowspan="1" style="vertical-align: top;"><input style="width: 250px;" id="frmTgt" name="tgthost" value="localhost:28117"></td>
      </tr>
      <tr>
        <td style="vertical-align: middle; text-align: right;">Log
Host<br>
        </td>
        <td colspan="3" rowspan="1" style="vertical-align: top;"><input style="width: 250px;" id="frmLog" name="loghost" value="localhost:28217"><br>
        </td>
      </tr>
      <tr>
        <td colspan="4" rowspan="1" style="vertical-align: top;"><br>
        </td>
      </tr>
      <tr>
        <td colspan="4" rowspan="1" style="vertical-align: top;">Clone/Reshard
Options<br>
        </td>
      </tr>
      <tr>
        <td style="text-align: right;">Namespace<br>
        </td>
        <td colspan="3" rowspan="1"><input style="width: 250px;" id="frmTargetns" name="targetns" value="resharder.clone"><br>
        </td>
      </tr>
      <tr>
        <td style="text-align: right;">Read Batch<br>
        </td>
        <td><input style="width: 25px;" id="frmReadBatch" name="readBatch" value="100"><br>
        </td>
        <td style="vertical-align: middle; text-align: right;">Reshard<br>
        </td>
        <td style="vertical-align: top;"><input id="frmReshard" name="frmReshard" class="frmReshard" value="false" type="checkbox"><br>
        </td>
      </tr>
      <tr>
        <td style="text-align: right;">Write Batch<br>
        </td>
        <td><input style="width: 25px;" id="frmWriteBatch" name="writeBatch" value="50"><br>
        </td>
        <td style="vertical-align: middle; text-align: right; width: 100px;">Shard
Key<br>
        </td>
        <td style="vertical-align: top;"><input id="frmKey" name="key"><br>
        </td>
      </tr>
      <tr>
        <td style="vertical-align: top; text-align: right;">Readers<br>
        </td><td style="vertical-align: top;"><input style="width: 25px;" id="frmNumReaders" name="frmNumReaders" value="1"></td>

        <td colspan="1" rowspan="2" style="vertical-align: middle; text-align: right;">&nbsp;Read&nbsp; from Secondary<br>
        </td>
        <td colspan="1" rowspan="2" style="vertical-align: middle;"><input id="cbxSecondary" name="cbxSecondary" type="checkbox"></td>
      </tr>
      <tr>
        <td style="vertical-align: top;"><br>
        </td>
        <td style="vertical-align: top; text-align: right;">Writers<br>
        </td>
        <td style="vertical-align: top;"><input style="width: 25px;" id="frmNumWriters" name="frmNumWriters" value="1"></td>
      </tr>
<tr>
        <td colspan="5" rowspan="1"><button type="submit" name="submit" class="button" id="submit_btn" value="Submit">Reshard</button><br>
        </td>
      </tr>
    </tbody>
  </table>
</form>
</div>

<div id="dialog-message" title="Collection Synchronized">
    <span class="ui-state-default"><span class="ui-icon ui-icon-info" style="margin: 0pt 7px 0pt 0pt; float: left;"></span></span>
    <div style="margin-left: 23px;">
        <p>
            The target collection is now synchronized.
            <br><br>
            Click STOP to terminate the Oplog Readers and reset the shard configuration.<br><br>
        </p></div>
</div>

<script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js?lang=css&amp;skin=sunburst">
</script>
<script src="/jquery-2.0.3.min.js"> </script>
<script src="/jquery-ui-1.10.4.custom.min.js"></script>
<script src="/jquery.jsPlumb-1.5.5-min.js"></script>
<script src="/form.js"></script>
<script type="text/javascript">
if (window.addEventListener) { // Mozilla, Netscape, Firefox
    window.addEventListener('load', WindowLoad, false);
} else if (window.attachEvent) { // IE
    window.attachEvent('onload', WindowLoad);
}

function WindowLoad(event) {
    	var json = document.getElementById("json").innerHTML;
    	json = JSON.parse(json);
	document.getElementById("json").innerHTML = JSON.stringify(json,null,2);
	
	$('#frmReshard').click(
					function() {
						if (this.checked) {
							$("#frmReshard").val("true");
						} else {
							$("#frmReshard").val("false");
						}
					});
					
	$('#cbxSecondary').click(
					function() {
						if (this.checked) {
							$("#cbxSecondary").val("true");
						} else {
							$("#cbxSecondary").val("false");
						}
					});
}
  </script>
<script src="http://code.highcharts.com/highcharts.js"></script>
<script src="http://code.highcharts.com/modules/exporting.js"></script>
</body></html>