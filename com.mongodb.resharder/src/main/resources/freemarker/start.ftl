<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html><head>
<meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">
  <title>MongoDB Resharder</title>
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

	json = {"mongos":"${mongos}","shards":"${numShards}","replSets":"${numRepl}","servers":"${numServers}"};
	document.getElementById("config").innerHTML = JSON.stringify(json,null,2);
}
  </script>
  
  <link rel="stylesheet" type="text/css" href="/styles.css">
  <title></title>

  
</head><body>
<style type="text/css">
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
</style>
<div id="messages" class="messageLog" style="display: none";></div>
<script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js?lang=css&amp;skin=sunburst">
</script>

<script src="/jquery-2.0.3.min.js"> </script>
<script src="/form.js"></script>

<script type="text/javascript">
var auto_refresh = setInterval(function (){
		$('.messageLog').load('/getStatus', function(data){document.getElementById("term").innerHTML += data});
	}, 1000); // refresh every second
</script>

<table style="text-align: left; width: 100%; margin-left: auto; margin-right: auto;" border="0" cellpadding="0" cellspacing="0">
  <tbody>
    <tr>
      <td colspan="3" rowspan="1" style="vertical-align: top; text-align: center;"><big><big><big>MongoDB
Resharder<br>
      </big></big></big><br>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top;"><br>
      </td>
      <td style="vertical-align: top; width: 25px;"><br>
      </td>
      <td style="vertical-align: top;"><br>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top; text-align: center;"><br>
      </td>
      <td style="vertical-align: top;"><br>
      </td>
      <td style="vertical-align: top;"><br>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top; text-align: center;"><br>
      </td>
      <td style="vertical-align: top;"><br>
      </td>
      <td style="vertical-align: top; text-align: center;"><br>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top; text-align: center;">
      <table style="text-align: left; width: 100%; margin-left: auto; margin-right: auto;" border="0" cellpadding="0" cellspacing="0">
        <tbody>
          <tr>
            <td colspan="2" rowspan="1" style="vertical-align: top; text-align: center;">Shard
Configuration</td>
          </tr>
          <tr>
            <td colspan="2" rowspan="1">
            <pre id="config" class="prettyprint"><br></pre>
            <br>
            </td>
          </tr>
          <tr>
            <td><br>
            </td>
            <td style="vertical-align: top;"><br>
            </td>
          </tr>
          <tr>
            <td colspan="2" rowspan="1" style="vertical-align: top; text-align: center;">Sharding
Status</td>
          </tr>
          <tr>
            <td colspan="2" rowspan="1" style="vertical-align: top;">
            <pre id="json" class="prettyprint">${collections}</pre>
            <br>
            <br>
            </td>
          </tr>
        </tbody>
      </table>
      <br>
      </td>
      <td style="vertical-align: top;"><br>
      </td>
      <td width="50%" height="30%" colspan="1" rowspan="2" style="vertical-align: top; text-align: center;">
      <div id="formDiv">
      <form name="reshard" action="">
        <table style="width: 512px; height: 177px;">
          <tbody>
            <tr>
              <td colspan="3" rowspan="1">Configure Resharder</td>
            </tr>
            <tr>
              <td style="text-align: center;">Select
Collection<br>
              </td>
              <td style="text-align: right;">Host<br>
              </td>
              <td><input id="frmHost" name="host" value="localhost:28017"><br>
              </td>
            </tr>
            <tr>
              <td colspan="1" rowspan="3">
              <#list collList as coll>
              <p><input name="namespace" id="frmNamespace" value="${coll}" type="radio">${coll}
              </p>
              </#list><br>
              </td>
              <td style="text-align: right;">Namespace<br>
              </td>
              <td><input id="frmTargetns" name="targetns" value="resharder.clone"><br>
              </td>
            </tr>
            <tr>
              <td style="text-align: right;">Read Batch<br>
              </td>
              <td><input id="frmReadBatch" name="readBatch" value="100"><br>
              </td>
            </tr>
            <tr>
              <td style="text-align: right;">Write Batch<br>
              </td>
              <td><input id="frmWriteBatch" name="writeBatch" value="50"><br>
              </td>
            </tr>
            <tr>
              <td colspan="3" rowspan="1"><button type="submit" name="submit" class="button" id="submit_btn" value="Submit">Reshard</button><br>
              </td>
            </tr>
            <tr>
              <td colspan="3" rowspan="1" style="vertical-align: top;"><br>
              </td>
            </tr>
          </tbody>
        </table>
      </form>
      </div>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top;"><br>
      </td>
      <td style="vertical-align: top;">
<div class="status" id="term"></div>
      </td>
    </tr>
    <tr>
      <td><br>
      </td>
      <td><br>
      </td>
      <td><br>
      </td>
    </tr>
    <tr>
      <td><br>
      </td>
      <td><br>
      </td>
      <td><br>
      </td>
    </tr>
  </tbody>
</table>

<br>

<br>

</body></html>