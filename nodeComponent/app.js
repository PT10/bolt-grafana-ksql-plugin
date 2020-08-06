const http = require('http');
const WebSocketServer = require('websocket').server;
const SimpleNodeLogger = require('simple-node-logger');
const opts = {
  logFilePath:'app.log',
  timestampFormat:'YYYY-MM-DD HH:mm:ss.SSS'
}

const log = SimpleNodeLogger.createSimpleLogger( opts );
log.setLevel('debug');

const port = 3001;
let activeReqSeq = 0;

const server = http.createServer();
server.listen(port);

const wsServer = new WebSocketServer({
  httpServer: server
});

wsServer.on('request', function(request) {
  const connection = request.accept(null, request.origin);
  connection.on('message', function(message) {
    const reqData = JSON.parse(message.utf8Data);
    log.info("Running panel id: " + reqData.panelId + " query: ", reqData.query);

    let req; 

    const options = {
      host: 'localhost',
      protocol: 'http:',
      path: '/query',
      port:'9099',
      method: 'POST',
      headers: {'Content-Type': 'application/json'}
    }

    callback = function(response) {
      log.info("Connection established for panel: " + reqData.panelId)
      response.on('data', function (chunk) {
        const data = chunk.toString();
        if (!connection.connected) {
          log.info("Client has disconnected hence closing panelId: " + reqData.panelId + " connection to ksql")
          req.destroy();
          return;
        }
        if (data.match(/\w+/)) {
          log.debug("Response for panelId: " + reqData.panelId + ". Data: " + data);
          connection.sendUTF(JSON.stringify({active: activeReqSeq, data: data}));
        } else {
          //console.log("Empty response: " + data);
        }
      });
    
      response.on('end', function () {
        log.info("Query finished for panelId: " + reqData.panelId)
      });
    }

    req = http.request(options, callback);
    req.write(JSON.stringify(reqData.query));
    req.end();

    req.on('error', (err) => {
      log.error("Error in connection for panel: " + reqData.panelId + " Error:" + err);
    })
  })
  connection.on('close', function(reasonCode, description) {
    log.info('Client has disconnected. Reason: ' + reasonCode + ' Description: ' + description);
  });
});