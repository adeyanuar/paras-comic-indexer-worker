const worker = require('./workerV3')
const { getLastUpdate } = require('./last-update')
const express = require('express')

const app = express()

worker()

app.get('/last-update', function(req, res){
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.write(JSON.stringify(getLastUpdate()))
    res.end()
})

var server = app.listen(9003, function () {
    var host = server.address().address
    var port = server.address().port
    
    console.log("server already running at http://%s:%s", host, port)
})