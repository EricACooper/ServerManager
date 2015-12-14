var Hapi = require('hapi');
var Boom = require('boom');
var server = new Hapi.Server();
var couchbase = require('couchbase');
var cluster = new couchbase.Cluster('couchbase://127.0.0.1');
var bucket = cluster.openBucket("QE-server-pool")


String.prototype.format = function() {
    var formatted = this;
    for( var arg in arguments ) {
        formatted = formatted.replace("{" + arg + "}", arguments[arg]);
    }
    return formatted;
};


/*

/getserver/username?count=number,os=centos,ver=6,expiresin=30
/deleteserver/username

*/


var serverPool = [ ];

var server_allocated = [];


var interval = setInterval(function() {
  var now = new Date();
  var now_seconds = now.getTime() / 1000;
  console.log("now_seconds",now_seconds);

  for (var i = 0; i < serverPool.length; i++) {
      if (serverPool[i].state === 'booked') {
        var expires = new Date(serverPool[i].expires_at);
        var expires_seconds = expires.getTime()/1000
        console.log( "expires_seconds", expires_seconds)
        if (expires_seconds <= now_seconds) {
          serverPool[i].state = 'available';
          serverPool[i].username = '';
          serverPool[i].expires_at = '';
        }
        
      }
  }
}, 1000000);


server.connection ({
        host: '172.23.105.177',
        port: Number(process.argv[2] || 8082)
    });

server.route({path: '/showall', method:'GET', handler: showall});
server.route({path: '/getservers/{username}', method:'GET', handler: getservers});
server.route({path: '/releaseservers/{username}', method:'GET', handler: releaseservers});
server.route({path: '/getavailablecount/{os}', method:'GET', handler: getavailablecount});
server.route({path: '/addserver/{ip}', method:'GET', handler: addserver});
server.route({path: '/removeserver/{ip}', method:'GET', handler: removeserver});



function getavailablecount(request,reply) {

  //console.log( "here is a string os {0}".format(request.params.os ));
  var poolId = request.query.poolId || '12hour';
  var N1QLQuery =  couchbase.N1qlQuery; 
  var queryString = "SELECT count(*) FROM `QE-server-pool` where state ='available' and os ='{0}' and poolId ='{1}'".format(request.params.os, poolId);

  console.log( queryString );
  var query = N1QLQuery.fromString( queryString );

  bucket.query(query,function(err,result){
        console.log( err );
	if (err) throw err;
	console.log("Result:", result[0]);
        reply( result[0]['$1'] );
   });


}


/*
/addserver/ip=number&os=centos&ver=60
*/

function addserver(request,reply) {
    console.log('in add server');
    console.log(request.params.ip);
    console.log(request.query.os);
    console.log(request.query.version);
    bucket.upsert(request.params.ip, {ipaddr:request.params.ip,OS:request.query.os,version:request.query.version,state:'available'}, function(err, result) {
          if (err) throw err;
    })

    //serverPool.push( { 'ipaddr': request.params.ip, 'username': '' , 'os': request.query.os , 'ver': request.query.version , state: 'available' } )
    // need to persist it
    reply();
}


/*
/removeserver/ip
*/

function removeserver(request,reply) {
    console.log('in remove server');
    console.log(request.params.ip);
    bucket.delete(request.params.ip, function(err, result) {
          if (err) throw err;
    })

    reply();
}


/*
/getservers/username?count=number&os=centos&ver=6&expiresin=30
*/

function getservers(request,reply) {
  var date = new Date();
  var serverlist = [];
  var requestCount = parseInt(request.query.count || 1);
  var expiresin = request.query.expiresin || 1; // minutes default = 1 hour
  var available = 0;
  var username = request.params.username || '';
  var expires_at = date.toJSON(date.setMinutes(date.getMinutes()+expiresin));
  var poolId = request.query.poolId || '12hour';
  var dontReserve = request.query.dontReserve || false;


  console.log(date);

  console.log(request.query );

  var N1QLQuery =  couchbase.N1qlQuery;
  var countQueryString =  "SELECT count(*) FROM `QE-server-pool` where state ='available' and os = '{0}' and poolId = '{1}'".format(request.query.os, poolId);
  console.log( countQueryString );
  var countQuery = N1QLQuery.fromString( countQueryString );

  var getServersQueryString =  "SELECT ipaddr FROM `QE-server-pool` where state ='available' and os = '{0}' limit {1}".format(
              request.query.os, requestCount );
  console.log(getServersQueryString);
  var getServersQuery = N1QLQuery.fromString( getServersQueryString );


  console.log('requested count', requestCount );

  bucket.query(countQuery,function(err,result){
        console.log('count query result', result);
        if (err) throw err;
        var availableCount = parseInt( result[0]['$1'] )
        if (requestCount > availableCount) {
                console.log('not enough servers');
    		// Reply with 403 request declined
    		var error = Boom.badRequest('No resource left');
    		error.output.statusCode = 499;    // Assign a custom error code
    		error.reformat();
    		error.output.payload.custom = 'The current number of servers requested is not available'; // Add custom key
                reply(error);
        } else {
           // get the servers
           bucket.query(getServersQuery,function(err1,result){
        	console.log('get servers query result', result);
                var serverList = [];
        	if (err1) throw err1;
                for (s in result) {
                    serverList.push(result[s]['ipaddr']);
                    var updateString = "update `QE-server-pool` set state='booked',username='`".concat(username).concat("`' where ipaddr='").concat(result[s]['ipaddr']).concat("'");
                    console.log('update string', updateString);
                    var updateServerRequest = N1QLQuery.fromString( updateString );
                    bucket.query(updateServerRequest,function(err1,result){
                	console.log('update servers result', result);
                        if (err1) throw err1;
                    });
                 }
                 reply(serverList)
           });
        }
   });
}



function getservers1(request,reply) {
  var date = new Date();
  var serverlist = [];
  var count = request.query.count || 1;
  var expiresin = request.query.expiresin || 1; // minutes default = 1 hour
  var available = 0;
  var username = request.params.username || '';
  var expires_at = date.toJSON(date.setMinutes(date.getMinutes()+expiresin));
  console.log(date);
  

  for (var i = 0; i < serverPool.length; i++) {
    if (serverPool[i].state === 'available') {
      available++;
    }
  };

  if (count <= available) {
    for (var i = 0; i < serverPool.length; i++) {
      if (serverPool[i].state === 'available') {
        serverPool[i].state = 'booked';
        serverPool[i].username = username;
        serverPool[i].expires_at = expires_at;
        serverlist.push(serverPool[i]);    
      }

      if (serverlist.length == count) {
        break;
      }
    }
    reply(serverlist);
  } else {
    // Reply with 403 request declined
    var error = Boom.badRequest('No resource left');
    error.output.statusCode = 499;    // Assign a custom error code
    error.reformat();

    error.output.payload.custom = 'The current number of VMs requested is not available'; // Add custom key

    reply(error);
    
  }

  

}

function releaseservers(request,reply){
  var username = request.params.username;
  var N1QLQuery =  couchbase.N1qlQuery;
  var updateString = "update `QE-server-pool` set state='available',username='' where username='`".concat(username).concat("`'");
  console.log('update string', updateString);
  var updateServerRequest = N1QLQuery.fromString( updateString );
  bucket.query(updateServerRequest,function(err1,result){
                        console.log('update servers result', result);
                        if (err1) throw err1;
                reply();
      });
}

function showall (request, reply) {
        console.log('in showall');
        var N1QLQuery =  couchbase.N1qlQuery;
        var showallString = "select * from `QE-server-pool`";
        var showallRequest = N1QLQuery.fromString( showallString );
        bucket.query(showallRequest,function(err1,result){
                        console.log('show all result', result);
                        if (err1) throw err1;
                reply(result);
        });
    }



server.start( function() {
        console.log('Server running at:', server.info.uri);
});

