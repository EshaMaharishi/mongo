// Wrap whole file in a function to avoid polluting the global namespace
(function() {

MongoRunner.runMongos = function( opts ){

    opts = opts || {}
    var useHostName = false;
    var runId = null;
    var waitForConnect = true;
    var fullOptions = opts;

    if( isObject( opts ) ) {

        opts = MongoRunner.mongosOptions( opts );
        fullOptions = opts;

        useHostName = opts.useHostName || opts.useHostname;
        runId = opts.runId;
        waitForConnect = opts.waitForConnect;

        opts = MongoRunner.arrOptions( "mongos", opts )
    }

    var mongos = MongoRunner.run(opts, waitForConnect)["conn"];

    if( !mongos ) 
        mongos = {}; 

    if( mongos ){
        mongos.commandLine = MongoRunner.arrToOpts( opts )
        mongos.name = (useHostName ? getHostName() : "localhost") + ":" + mongos.commandLine.port
        mongos.host = mongos.name
        mongos.port = parseInt( mongos.commandLine.port )
        mongos.runId = runId || ObjectId()
        mongos.savedOptions = MongoRunner.savedOptions[ mongos.runId ]
        mongos.fullOptions = fullOptions;
    }

    return mongos;
}

MongoRunner.mongosOptions = function( opts ){

    opts = MongoRunner.mongoOptions( opts )

    // Normalize configdb option to be host string if currently a host
    if( opts.configdb && opts.configdb.getDB ){
        opts.configdb = opts.configdb.host
    }

    opts.pathOpts = Object.merge( opts.pathOpts,
                                { configdb : opts.configdb.replace( /:|,/g, "-" ) } )

    if( ! opts.logFile && opts.useLogFiles ){
        opts.logFile = MongoRunner.toRealFile( "$dataDir/mongos-$configdb-$port.log",
                                               opts.pathOpts )
    }
    else if( opts.logFile ){
        opts.logFile = MongoRunner.toRealFile( opts.logFile, opts.pathOpts )
    }

    if ( opts.logFile !== undefined ){
        opts.logpath = opts.logFile;
    }

    if( jsTestOptions().keyFile && !opts.keyFile) {
        opts.keyFile = jsTestOptions().keyFile
    }

    return opts
}

/**
 * DEPRECATED
 *
 * Use MongoRunner.runMongos instead.
 */
startMongos = function(args){
    return MongoRunner.runMongos(args);
}


}());
