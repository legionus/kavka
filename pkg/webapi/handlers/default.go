package handlers

import (
	"net/http"

	"github.com/legionus/kavka/pkg/api"
	"github.com/legionus/kavka/pkg/context"
)

func defultHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	content := `<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <link href="http://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Endpoints | Kavka API v1</title>
  </head>
  <body>
    <div class="container"><h2>Kavka API v1</h2><br>
        <table class="table">
          <tr class="info"><td colspan="3"><h4>Raw API</h4></td></tr>
          <tr>
            <th class="text-right">Write raw message to Kavka</th>
            <td>POST</td>
            <td><code>{schema}://{host}`+api.TopicsPath+`/{topic}/{partition}</code></td>
          </tr>
          <tr>
            <th class="text-right">Read from Kavka by absolute position</th>
            <td>GET</td>
            <td>
               <code>{schema}://{host}`+api.TopicsPath+`/{topic}/{partition}?offset={offset}</code>
            </td>
          </tr>
          <tr>
            <th class="text-right">Read data relative to the beginning or end of the queue</th>
            <td>GET</td>
            <td>
               <p><code>{schema}://{host}`+api.TopicsPath+`/{topic}/{partition}?relative={position}</code></p>
               The <b>{position}</b> can be positive or negative.
            </td>
          </tr>
          <tr class="info"><td colspan="3"><h4>JSON API</h4></td></tr>
          <tr>
            <th class="text-right">Write to Kavka</th>
            <td>POST</td>
            <td><code>{schema}://{host}`+api.JSONTopicsPath+`/{topic}/{partition}</code></td>
          </tr>
          <tr>
            <th class="text-right">Read from Kavka by absolute position</th>
            <td>GET</td>
            <td>
               <code>{schema}://{host}`+api.JSONTopicsPath+`/{topic}/{partition}?offset={offset}&limit={limit}</code>
            </td>
          </tr>
          <tr>
            <th class="text-right">Read data relative to the beginning or end of the queue</th>
            <td>GET</td>
            <td>
               <p><code>{schema}://{host}`+api.JSONTopicsPath+`/{topic}/{partition}?relative={position}&limit={limit}</code></p>
               The <b>{position}</b> can be positive or negative.
            </td>
          </tr>
          <tr class="info"><td colspan="3"><h4>Infomation about topics and partitions</h4></td></tr>
          <tr>
            <th class="text-right">Obtain topic list</th>
            <td>GET</td>
            <td><code>{schema}://{host}`+api.InfoTopicsPath+`</code></td>
          </tr>
          <tr>
            <th class="text-right">Obtain information about all partitions in topic</th>
            <td>GET</td>
            <td><code>{schema}://{host}`+api.InfoTopicsPath+`/{topic}</code></td>
          </tr>
          <tr>
            <th class="text-right">Obtain information about partition</th>
            <td>GET</td>
            <td><code>{schema}://{host}`+api.InfoTopicsPath+`/{topic}/{partition}</code></td>
          </tr>
          <tr class="info"><td colspan="3"><h4>Blob API</h4></td></tr>
          <tr>
            <th class="text-right">Obtain blob directly</th>
            <td>GET</td>
            <td><code>{schema}://{host}`+api.BlobsPath+`/{digest}</code></td>
          </tr>
          <tr class="info"><td colspan="3"><h4>Infomation about etcd cluster members</h4></td></tr>
          <tr>
            <th class="text-right">Obtain information about members</th>
            <td>GET</td>
            <td><code>{schema}://{host}`+api.EtcdMembersPath+`</code></td>
          </tr>
          <tr>
            <th class="text-right">Adds a new member</th>
            <td>POST</td>
            <td><code>{schema}://{host}`+api.EtcdMembersPath+`</code></td>
          </tr>
          <tr>
            <th class="text-right">Updates the peer addresses of the member</th>
            <td>POST</td>
            <td><code>{schema}://{host}`+api.EtcdMembersPath+`/{memberid}</code></td>
          </tr>
          <tr>
            <th class="text-right">Removes an existing member</th>
            <td>DELETE</td>
            <td><code>{schema}://{host}`+api.EtcdMembersPath+`/{memberid}</code></td>
          </tr>
        </table>
    </div>
  </body>
</html>`
	w.Write([]byte(content))
}
