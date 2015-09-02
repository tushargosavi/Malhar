/*
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Functions fro charting top url table.
 */

function DrawTopUrlTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        topUrlTable = new google.visualization.DataTable();
        topUrlTable.addColumn('string', 'URL');
        topUrlTable.addColumn('number', 'requests/sec');
        topUrlTable.addRows(10);
        for(var i=0; (i <  pts.length)&&(i < 10); i++) 
        {
          var row = pts[i].split("##");
          topUrlTable.setCell(i, 0, row[0]);
          topUrlTable.setCell(i, 1, parseInt(row[1]));
          delete pts[i];
        }
        topUrlTableChart.draw(topUrlTable, {showRowNumber: true});
        delete topUrlTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "TopUrlData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
