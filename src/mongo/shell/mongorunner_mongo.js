// Wrap whole file in a function to avoid polluting the global namespace
(function() {

MongoRunner.runMongo = function(){
    var args = argumentsToArray( arguments );
    var progName = args[0];

    if ( jsTestOptions().auth ) {
        args = args.slice(1);
        args.unshift(progName,
                     '-u', jsTestOptions().authUser,
                     '-p', jsTestOptions().authPassword,
                     '--authenticationMechanism', DB.prototype._defaultAuthenticationMechanism,
                     '--authenticationDatabase=admin');
    }

    if (!_useWriteCommandsDefault())
        args.unshift("mongo", '--useLegacyWriteOps');

    MongoRunner.run( args, false );
}

MongoRunner.mongoOptions = function( opts ){

    // Don't remember waitForConnect
    var waitForConnect = opts.waitForConnect;
    delete opts.waitForConnect;

    // If we're a mongo object
    if( opts.getDB ){
        opts = { restart : opts.runId }
    }

    // Initialize and create a copy of the opts
    opts = Object.merge( opts || {}, {} )

    if( ! opts.restart ) opts.restart = false

    // RunId can come from a number of places
    // If restart is passed as an old connection
    if( opts.restart && opts.restart.getDB ){
        opts.runId = opts.restart.runId
        opts.restart = true
    }
    // If it's the runId itself
    else if( isObject( opts.restart ) ){
        opts.runId = opts.restart
        opts.restart = true
    }

    if( isObject( opts.remember ) ){
        opts.runId = opts.remember
        opts.remember = true
    }
    else if( opts.remember == undefined ){
        // Remember by default if we're restarting
        opts.remember = opts.restart
    }

    // If we passed in restart : <conn> or runId : <conn>
    if( isObject( opts.runId ) && opts.runId.runId ) opts.runId = opts.runId.runId

    if( opts.restart && opts.remember ) opts = Object.merge( MongoRunner.savedOptions[ opts.runId ], opts )

    // Create a new runId
    opts.runId = opts.runId || ObjectId();

    // Save the port if required
    if( ! opts.forgetPort ) opts.port = opts.port || MongoRunner.nextOpenPort()

    var shouldRemember = ( ! opts.restart && ! opts.noRemember ) || ( opts.restart && opts.appendOptions )

    // Normalize and get the binary version to use
    opts.binVersion = MongoRunner.getBinVersionFor(opts.binVersion);

    if ( shouldRemember ){
        MongoRunner.savedOptions[ opts.runId ] = Object.merge( opts, {} )
    }

    // Default for waitForConnect is true
    opts.waitForConnect = (waitForConnect == undefined || waitForConnect == null) ?
        true : waitForConnect;

    if( jsTestOptions().useSSL ) {
        if (!opts.sslMode) opts.sslMode = "requireSSL";
        if (!opts.sslPEMKeyFile) opts.sslPEMKeyFile = "jstests/libs/server.pem";
        if (!opts.sslCAFile) opts.sslCAFile = "jstests/libs/ca.pem";
        opts.sslWeakCertificateValidation = "";
        opts.sslAllowInvalidCertificates = "";
    }

    if ( jsTestOptions().useX509 && !opts.clusterAuthMode ) {
        opts.clusterAuthMode = "x509";
    }

    opts.port = opts.port || MongoRunner.nextOpenPort()
    MongoRunner.usedPortMap[ "" + parseInt( opts.port ) ] = true

    opts.pathOpts = Object.merge( opts.pathOpts || {}, { port : "" + opts.port, runId : "" + opts.runId } )

    return opts
}

/**
 * DEPRECATED
 *
 * Use one of the MongoRunner.runMongoX (ie, MongoRunner.runMongo) functions instead.
 *
 * Start a mongo program instance.  This function's first argument is the
 * program name, and subsequent arguments to this function are passed as
 * command line arguments to the program.  Returns pid of the spawned program.
 */
startMongoProgramNoConnect = function() {
    var args = argumentsToArray( arguments );
    var progName = args[0];

    if ( jsTestOptions().auth ) {
        args = args.slice(1);
        args.unshift(progName,
                     '-u', jsTestOptions().authUser,
                     '-p', jsTestOptions().authPassword,
                     '--authenticationMechanism', DB.prototype._defaultAuthenticationMechanism,
                     '--authenticationDatabase=admin');
    }

    if (progName == 'mongo' && !_useWriteCommandsDefault()) {
        args = args.slice(1);
        args.unshift(progName, '--useLegacyWriteOps');
    }

    return MongoRunner.run( args, false )["pid"];
}

}());
