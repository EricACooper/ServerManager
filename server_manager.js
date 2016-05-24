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



server.connection ({
        host: '172.23.105.177',
        port: Number(process.argv[2] || 8082)
    });

server.route({path: '/showall', method:'GET', handler: showall});
server.route({path: '/getservers/{username}', method:'GET', handler: getservers});
server.route({path: '/releaseservers/{username}/{state}', method:'GET', handler: releaseservers});
server.route({path: '/getavailablecount/{os}', method:'GET', handler: getavailablecount});
server.route({path: '/addserver/{ip}', method:'GET', handler: addserver});
server.route({path: '/removeserver/{ip}', method:'GET', handler: removeserver});


server.route({path: '/getdockers/{username}', method:'GET', handler: getdockers});
server.route({path: '/releasedockers/{username}', method:'GET', handler: releasedockers});





function releasedockers(request,reply) {

  var ipaddr = request.query.ipaddr || 'ipnotgiven';
  var username = request.params.username || 'anonymous';
  var N1QLQuery =  couchbase.N1qlQuery;
  var queryString = "SELECT ipaddr,availableServers,users FROM `QE-server-pool` where ipaddr = '{0}'".format(ipaddr);

  console.log( Date().toLocaleString() + " releasedockers:" + queryString );
  var query = N1QLQuery.fromString( queryString );

  bucket.query(query,function(err,results){
        console.log( err );
        if (err) throw err;
        console.log("Result:", results);
        if (results.length > 0) {
              var users = results[0]['users'];
              var newCount = results[0]['availableServers'] + users[username];
              delete users[username];
              console.log("users are ", JSON.stringify(users));

              // update the record
              var updateString = "update `QE-server-pool` set availableServers={0}, users={1} where ipaddr='{2}';"
                          .format(newCount, JSON.stringify(users), ipaddr);
                                                                                                          

              var updateServerRequest = N1QLQuery.fromString( updateString );
              bucket.query(updateServerRequest,function(err1,result){
                        if (err1) throw err1; });
                reply( );
                return;

        } else {
            var error = Boom.badRequest('Unknown server');
            error.output.statusCode = 499;    // Assign a custom error code
            error.reformat();
            error.output.payload.custom = 'Unknown server';
            reply(error);
        }

       });
  }




function getdockers(request,reply) {

  var poolId = request.query.poolId || '12hour';
  var username = request.params.username || 'anonymous';
  var count = parseInt( request.query.count );
  var N1QLQuery =  couchbase.N1qlQuery;
  var queryString = "SELECT ipaddr,availableServers,users FROM `QE-server-pool` where serverType = 'docker' and poolId = '{0}'".format(poolId);

  console.log( Date().toLocaleString() + " getavailabledockercount:" + queryString );
  var query = N1QLQuery.fromString( queryString );


  // later optimization is best fit, load balancing

  bucket.query(query,function(err,results){
        console.log( err );
        if (err) throw err;
        console.log("Result:", results);
        for (res in results) {
            console.log("Result:", results[res]);
            console.log("Result:", results[res]['availableServers']);
            if (results[res]['availableServers'] >= count) {

                console.log("incoming users are", results[res]['users'] );
                var users = results[res]['users'];
                users[username] = count;
                var availableServers = results[res]['availableServers'] - count;
                console.log("users are ", JSON.stringify(users));

                // update the record
                var updateString = "update `QE-server-pool` set availableServers={0}, users={1} where ipaddr='{2}';"
                          .format(availableServers, JSON.stringify(users), results[res]['ipaddr'] );
                console.log("update string", updateString);
                var updateServerRequest = N1QLQuery.fromString( updateString );
                bucket.query(updateServerRequest,function(err1,result){
                        if (err1) throw err1; });
                reply( results[res]['ipaddr'] );
                return;
            }
        }
        // dropthrough means we there is no available Dockers
        var error = Boom.badRequest('No resource left');
        error.output.statusCode = 499;    // Assign a custom error code
        error.reformat();
        error.output.payload.custom = 'The current number of servers requested is not available'; // Add custom key
        reply(error);
   });


}






function getavailablecount(request,reply) {

  //console.log( "here is a string os {0}".format(request.params.os ));
  var poolId = request.query.poolId || '12hour';


  var dockerOS = request.query.os || 'centos';
  var N1QLQuery =  couchbase.N1qlQuery; 

  console.log( request.params.os );
  if (request.params.os.indexOf("docker") < 0) {
      var queryString = "SELECT count(*) FROM `QE-server-pool` where state ='available' and os ='{0}' and poolId ='{1}'".format(request.params.os, poolId);


      console.log( Date().toLocaleString() + " getavailablecount:" + queryString );
      var query = N1QLQuery.fromString( queryString );

      bucket.query(query,function(err,result){
            console.log( err );
	    if (err) throw err;
	    console.log("Result:", result[0]);
            reply( result[0]['$1'] );
       });
   } else {
        // have a docker request
      console.log("have a docker request");
      console.log(request.query.os);

      var queryString = "SELECT ipaddr,availableServers,users FROM `QE-server-pool` where serverType = 'docker' and os ='{0}'and poolId = '{1}'".format(request.query.os,poolId);

      console.log( Date().toLocaleString() + " getavailabledockercount:" + queryString );
      var query = N1QLQuery.fromString( queryString );
      var capacityCount = 0;
      bucket.query(query,function(err,results){
        console.log( err );
        if (err) throw err;
        console.log("Result:", results);
        for (res in results) {
            console.log("Result:", results[res]);
            console.log("Result:", results[res]['availableServers']);
            if (results[res]['availableServers'] >= capacityCount) {
                 capacityCount = results[res]['availableServers'];
            }
        }
        reply( capacityCount );
      });
    }
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

  console.log( Date().toLocaleString() + ' getservers');

  console.log(request.query );

  var N1QLQuery =  couchbase.N1qlQuery;
  var countQueryString =  "SELECT count(*) FROM `QE-server-pool` where state ='available' and os = '{0}' and poolId = '{1}'".format(request.query.os, poolId);
  console.log( countQueryString );
  var countQuery = N1QLQuery.fromString( countQueryString );

  var getServersQueryString =  "SELECT ipaddr,username FROM `QE-server-pool` where state ='available' and os = '{0}' and poolId = '{1}' limit {2}".format(
              request.query.os, poolId, requestCount );
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
                    //var updateString = "update `QE-server-pool` set state='booked',username='".concat(username).concat("' where ipaddr='").concat(result[s]['ipaddr']).concat("'");
                    var updateString = "update `QE-server-pool` set state='booked',username='{0}',prevUser='{1}' where ipaddr='{2}';"
                          .format(username,result[s]['username'], result[s]['ipaddr']);
                    console.log('update string', updateString);
                    var updateServerRequest = N1QLQuery.fromString( updateString );
                    bucket.query(updateServerRequest,function(err1,result){
                        if (err1) throw err1;
                    });
                 }
                 reply(serverList)
           });
        }
   });
}



function releaseservers(request,reply){
  var username = request.params.username;
  var state = request.params.state;
  var N1QLQuery =  couchbase.N1qlQuery;
  var updateString = "update `QE-server-pool` set state='{0}' where username='{1}' and state='booked'".format(state,username);
  console.log( Date().toLocaleString() + ' releaseservers:' + username + ' state:' + state);
  console.log('update string', updateString);
  var updateServerRequest = N1QLQuery.fromString( updateString );
  bucket.query(updateServerRequest,function(err1,result){
                        //console.log('update servers result', result);
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
        console.log(Date().toLocaleString() + 'Server running at:', server.info.uri);
});

