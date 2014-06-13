// Wrap whole file in a function to avoid polluting the global namespace
(function() {

/**
 * Called by MongoRunner.runMongoX functions to actually start the processes
 *
 * if waitForConnect true,
 *  @return a Mongo object connected to the started instance
 */
MongoRunner.run = function( argArray, waitForConnect ){
    argArray = appendSetParameterArgs(argArray);
    var port = _parsePort.apply(null, argArray);
    var pid = _startMongoProgram.apply(null, argArray);

    var conn = null;
    if (waitForConnect) {
        assert.soon( function() {
            try {
                conn = new Mongo("127.0.0.1:" + port);
                return true;
            } catch( e ) {
                if (!checkProgram(pid)) {
                    print("Could not start program at " + port + ", process ended")
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + port, 600 * 1000);
    }

    var ret = new Object();
    ret["conn"] = conn;
    ret["pid"] = pid;

    return ret;   
}

/**
 * Kills a mongod or mongos process.
 *
 * @param {number} port the port of the process to kill
 * @param {number} signal The signal number to use for killing
 * @param {Object} opts Additional options. Format:
 *    {
 *      auth: {
 *        user {string}: admin user name
 *        pwd {string}: admin password
 *      }
 *    }
 *
 * Note: The auth option is required in a authenticated mongod running in Windows since
 *  it uses the shutdown command, which requires admin credentials.
 */
MongoRunner.stop = function( port, signal, opts ){
    if( ! port ) {
        print( "Cannot stop mongo process " + port );
        return;
    }

    signal = signal || 15;

    if( port.port )
        port = parseInt( port.port );

    if( port instanceof ObjectId ){
        var opts = MongoRunner.savedOptions( port );
        if( opts ) port = parseInt( opts.port );
    }

    var exitCode = stopMongod( parseInt( port ), parseInt( signal ), opts );

    delete MongoRunner.usedPortMap[ "" + parseInt( port ) ];

    return exitCode;
}

MongoRunner.isStopped = function( port ){
    
    if( ! port ) {
        print( "Cannot detect if process " + port + " is stopped." )
        return
    }
    
    if( port.port )
        port = parseInt( port.port )
    
    if( port instanceof ObjectId ){
        var opts = MongoRunner.savedOptions( port )
        if( opts ) port = parseInt( opts.port )
    }
    
    return MongoRunner.usedPortMap[ "" + parseInt( port ) ] ? false : true
}

/**
 * DEPRECATED
 * 
 * Use MongoRunner.stop instead
 */
MongoRunner.stopMongod = function( port, signal, opts ){
    MongoRunner.stop( port, signal, opts );    
}

/**
 * DEPRECATED
 * 
 * Use MongoRunner.stop instead
 */
MongoRunner.stopMongos = MongoRunner.stop

/**
 * DEPRECATED -- was used to start mongod, mongos, or mongo instance
 *
 * Use one of the MongoRunner.runMongoX (ie, MongoRunner.runMongod) functions instead.
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
