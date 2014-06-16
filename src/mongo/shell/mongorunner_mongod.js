// Wrap whole file in a function to avoid polluting the global namespace
(function() {

/**
 * Starts a mongod instance.
 *
 * Input "opts" can be given either as array of command line arguments
 * or key/value pairs in a document
 *
 * @param {Object} opts
 *   {
 *     useHostName {boolean}: Uses hostname of machine if true
 *     forceLock {boolean}: Deletes the lock file if set to true
 *     dbpath {string}: location of db files
 *     cleanData {boolean}: Removes all files in dbpath if true
 *     startClean {boolean}: same as cleanData
 *     noCleanData {boolean}: Do not clean files (cleanData takes priority)
 *     @see MongoRunner.mongodOptions for other options
 *   }
 *
 * @return {Mongo} connection object to the started mongod instance.
 */

MongoRunner.runMongod = function ( opts, resetDbPath ){

    print("starting runMongod");

    opts = opts || {}
    var useHostName = false;
    var runId = null;
    var waitForConnect = true;
    var fullOptions = opts;

    if( opts instanceof Array ){
        printjson( opts );
        if( resetDbPath ){
            var dbpath = _parsePath.apply(null, opts);
            print( "Resetting db path '" + dbpath + "'" )
            resetDbpath( dbpath );
        }
        if( opts[0] != "mongod" )
            opts.unshift("mongod");
    }
    else if( isObject( opts ) ) {

        opts = MongoRunner.mongodOptions( opts );
        fullOptions = opts;

        useHostName = opts.useHostName || opts.useHostname;
        runId = opts.runId;
        waitForConnect = opts.waitForConnect;

        if( opts.forceLock ) removeFile( opts.dbpath + "/mongod.lock" )
        if( ( opts.cleanData || opts.startClean ) || ( ! opts.restart && ! opts.noCleanData ) ){
            print( "Resetting db path '" + opts.dbpath + "'" )
            resetDbpath( opts.dbpath )
        }

        // convert options object into an array of command line arguments to mongod
        // prepends binary name to array
        opts = MongoRunner.arrOptions( "mongod", opts )
    }

    opts = appendSetParameterArgs( opts );

    var port = _parsePort.apply(null, opts );
    var mongod = MongoRunner.run( opts, port, waitForConnect )["conn"];

    print("after MongoRunner.run");
    printjson( mongod );

    if( waitForConnect ){
        mongod.commandLine = MongoRunner.arrToOpts( opts );
        mongod.name = (useHostName ? getHostName() : "localhost") + ":" + mongod.commandLine.port;
        mongod.host = mongod.name;
        mongod.port = parseInt( mongod.commandLine.port );
        mongod.runId = runId || ObjectId();
        mongod.savedOptions = MongoRunner.savedOptions[ mongod.runId ];
        mongod.fullOptions = fullOptions;
    }
    return mongod;
}

/**
 * @option {object} opts
 *
 *   {
 *     dbpath {string}
 *     useLogFiles {boolean}: use with logFile option.
 *     logFile {string}: path to the log file. If not specified and useLogFiles
 *       is true, automatically creates a log file inside dbpath.
 *     noJournalPrealloc {boolean}
 *     noJournal {boolean}
 *     keyFile
 *     replSet
 *     oplogSize
 *   }
 */
MongoRunner.mongodOptions = function( opts ){

    opts = MongoRunner.mongoOptions( opts )

    opts.dbpath = MongoRunner.toRealDir( opts.dbpath || "$dataDir/mongod-$port",
                                         opts.pathOpts )

    opts.pathOpts = Object.merge( opts.pathOpts, { dbpath : opts.dbpath } )

    if( ! opts.logFile && opts.useLogFiles ){
        opts.logFile = opts.dbpath + "/mongod.log"
    }
    else if( opts.logFile ){
        opts.logFile = MongoRunner.toRealFile( opts.logFile, opts.pathOpts )
    }

    if ( opts.logFile !== undefined ) {
        opts.logpath = opts.logFile;
    }

    if( jsTestOptions().noJournalPrealloc || opts.noJournalPrealloc )
        opts.nopreallocj = ""

    if( jsTestOptions().noJournal || opts.noJournal )
        opts.nojournal = ""

    if( jsTestOptions().keyFile && !opts.keyFile) {
       opts.keyFile = jsTestOptions().keyFile
    }

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

    if( opts.noReplSet ) opts.replSet = null
    if( opts.arbiter ) opts.oplogSize = 1

    return opts
}





// The following deprecated functions for creating a mongod instance
// have all been rewritten to call MongoRunner.runMongod

/**
 * DEPRECATED
 *
 * Start a mongod instance and return a 'Mongo' object connected to it.
 * This function's arguments are passed as command line arguments to mongod.
 * The specified 'dbpath' is cleared if it exists, created if not.
 * var conn = startMongodEmpty("--port", 30000, "--dbpath", "asdf");
 */
startMongodEmpty = function () {
    var args = createMongoArgs( "mongod", arguments );
    args = args.slice(1); // remove "mongod" from head since it is added in MongoRunner.runMongod
    return MongoRunner.runMongod( args, true );
}

/**
 * DEPRECATED
 *
 * wipes data directory before starting
 */
startMongod = function () {
    var args = createMongoArgs( "mongod", arguments );
    args = args.slice(1); // remove "mongod" from head since it is added in MongoRunner.runMongod
    return MongoRunner.runMongod( args, true );
}

/**
 * DEPRECATED
 *
 * does not wipe data directory before starting
 */
startMongodNoReset = function(){
    var args = createMongoArgs( "mongod", arguments );
    args = args.slice(1); // remove "mongod" from head since it is added in MongoRunner.runMongod
    return MongoRunner.runMongod( args, false );
}

/**
 * DEPRECATED
 *
 * Use MongoRunner.runMongod instead
 *
 * After initializing a MongodRunner, you must call start() on it.
 * @param {int} port port to run db on, use allocatePorts(num) to requision
 * @param {string} dbpath path to use
 * @param {boolean} peer pass in false (DEPRECATED, was used for replica pair host)
 * @param {boolean} arbiter pass in false (DEPRECATED, was used for replica pair host)
 * @param {array} extraArgs other arguments for the command line
 * @param {object} options other options include no_bind to not bind_ip to 127.0.0.1
 *    (necessary for replica set testing)
 */
MongodRunner = function( port, dbpath, peer, arbiter, extraArgs, options ) {
    this.port_ = port;
    this.dbpath_ = dbpath;
    this.peer_ = peer;
    this.arbiter_ = arbiter;
    this.extraArgs_ = extraArgs;
    this.options_ = options ? options : {};
};

/**
 * DEPRECATED
 *
 * Use MongoRunner.runMongod instead
 * Start the mongod process.
 * @param {boolean} reuseData If the data directory should be left intact (default is to wipe it)
 */
MongodRunner.prototype.start = function( reuseData ) {
    var args = [];
    if ( reuseData ) {
        args.push( "mongod" );
    }
    args.push( "--port" );
    args.push( this.port_ );
    args.push( "--dbpath" );
    args.push( this.dbpath_ );
    args.push( "--nohttpinterface" );
    args.push( "--noprealloc" );
    args.push( "--smallfiles" );
    if (!this.options_.no_bind) {
      args.push( "--bind_ip" );
      args.push( "127.0.0.1" );
    }
    if ( this.extraArgs_ ) {
        args = args.concat( this.extraArgs_ );
    }
    removeFile( this.dbpath_ + "/mongod.lock" );
    if ( reuseData ) {
        return MongoRunner.runMongod( args, false );
    } else {
        return MongoRunner.runMongod( args, true );
    }
}

/**
 * DEPRECATED
 *
 * Do not use MongodRunner (note the 'd'). Use MongoRunner.runMongod instead.
 */
MongodRunner.prototype.port = function() { return this.port_; }

/**
 * DEPRECATED
 *
 * Do not use MongodRunner (note the 'd'). Use MongoRunner.runMongod instead.
 */
MongodRunner.prototype.toString = function() { return [ this.port_, this.dbpath_, this.peer_, this.arbiter_ ].toString(); }

/**
 * DEPRECATED
 *
 * Start mongod or mongos and return a Mongo() object connected to there.
 * This function's first argument is "mongod" or "mongos" program name, \
 * and subsequent arguments to this function are passed as
 * command line arguments to the program.
 */
startMongoProgram = function(){
    // Enable test commands.
    // TODO: Make this work better with multi-version testing so that we can support
    // enabling this on 2.4 when testing 2.6
    var args = argumentsToArray( arguments );
    return MongoRunner.run( args, true )["conn"];
}


}());
