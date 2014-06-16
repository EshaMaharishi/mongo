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
    var uses_port = argArray[0].indexOf("mongod") > -1 || argArray[0].indexOf("mongos") > -1;

    if( uses_port )
        var port = _parsePort.apply(null, argArray);

    var pid = _startMongoProgram.apply(null, argArray);

    var conn = null;
    if (uses_port && waitForConnect) {
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


_parsePath = function() {
    var dbpath = "";
    for( var i = 0; i < arguments.length; ++i )
        if ( arguments[ i ] == "--dbpath" )
            dbpath = arguments[ i + 1 ];

    if ( dbpath == "" )
        throw Error("No dbpath specified");

    return dbpath;
}

_parsePort = function() {
    var port = "";
    for( var i = 0; i < arguments.length; ++i )
        if ( arguments[ i ] == "--port" )
            port = arguments[ i + 1 ];

    if ( port == "" )
        throw Error("No port specified");
    return port;
}

myPort = function() {
    var m = db.getMongo();
    if ( m.host.match( /:/ ) )
        return m.host.match( /:(.*)/ )[ 1 ];
    else
        return 27017;
}

connectionURLTheSame = function( a , b ){

    if ( a == b )
        return true;

    if ( ! a || ! b )
        return false;

    if( a.host ) return connectionURLTheSame( a.host, b )
    if( b.host ) return connectionURLTheSame( a, b.host )

    if( a.name ) return connectionURLTheSame( a.name, b )
    if( b.name ) return connectionURLTheSame( a, b.name )

    if( a.indexOf( "/" ) < 0 && b.indexOf( "/" ) < 0 ){
        a = a.split( ":" )
        b = b.split( ":" )

        if( a.length != b.length ) return false

        if( a.length == 2 && a[1] != b[1] ) return false

        if( a[0] == "localhost" || a[0] == "127.0.0.1" ) a[0] = getHostName()
        if( b[0] == "localhost" || b[0] == "127.0.0.1" ) b[0] = getHostName()

        return a[0] == b[0]
    }
    else {
        var a0 = a.split( "/" )[0]
        var b0 = b.split( "/" )[0]
        return a0 == b0
    }
}

createMongoArgs = function( binaryName , args ){
    var fullArgs = [ binaryName ];

    if ( args.length == 1 && isObject( args[0] ) ){
        var o = args[0];
        for ( var k in o ){
          if ( o.hasOwnProperty(k) ){
            if ( k == "v" && isNumber( o[k] ) ){
                var n = o[k];
                if ( n > 0 ){
                    if ( n > 10 ) n = 10;
                    var temp = "-";
                    while ( n-- > 0 ) temp += "v";
                    fullArgs.push( temp );
                }
            }
            else {
                fullArgs.push( "--" + k );
                if ( o[k] != "" )
                    fullArgs.push( "" + o[k] );
            }
          }
        }
    }
    else {
        for ( var i=0; i<args.length; i++ )
            fullArgs.push( args[i] )
    }

    return fullArgs;
}

MongoRunner = function(){}

MongoRunner.dataDir = "/data/db";
MongoRunner.dataPath = "/data/db/";
MongoRunner.usedPortMap = {};
MongoRunner.savedOptions = {};

MongoRunner.VersionSub = function(regex, version) {
    this.regex = regex;
    this.version = version;
}

// These patterns allow substituting the binary versions used for each
// version string to support the dev/stable MongoDB release cycle.
MongoRunner.binVersionSubs = [ new MongoRunner.VersionSub(/^latest$/, ""),
                               new MongoRunner.VersionSub(/^oldest-supported$/, "1.8"),
                               // To-be-updated when 2.8 becomes available
                               new MongoRunner.VersionSub(/^last-stable$/, "2.6"),
                               // Latest unstable and next stable are effectively the
                               // same release
                               new MongoRunner.VersionSub(/^2\.7(\..*){0,1}/, ""),
                               new MongoRunner.VersionSub(/^2\.8(\..*){0,1}/, "") ];


MongoRunner.getBinVersionFor = function(version) {

    // If this is a version iterator, iterate the version via toString()
    if (version instanceof MongoRunner.versionIterator.iterator) {
        version = version.toString();
    }

    // No version set means we use no suffix, this is *different* from "latest"
    // since latest may be mapped to a different version.
    if (version == null) version = "";
    version = version.trim();
    if (version === "") return "";

    // See if this version is affected by version substitutions
    for (var i = 0; i < MongoRunner.binVersionSubs.length; i++) {
        var sub = MongoRunner.binVersionSubs[i];
        if (sub.regex.test(version)) {
            version = sub.version;
        }
    }

    return version;
}

MongoRunner.areBinVersionsTheSame = function(versionA, versionB) {

    versionA = MongoRunner.getBinVersionFor(versionA);
    versionB = MongoRunner.getBinVersionFor(versionB);

    if (versionA === "" || versionB === "") {
        return versionA === versionB;
    }

    return versionA.startsWith(versionB) ||
           versionB.startsWith(versionA);
}

MongoRunner.logicalOptions = { runId : true,
                               pathOpts : true,
                               remember : true,
                               noRemember : true,
                               appendOptions : true,
                               restart : true,
                               noCleanData : true,
                               cleanData : true,
                               startClean : true,
                               forceLock : true,
                               useLogFiles : true,
                               logFile : true,
                               useHostName : true,
                               useHostname : true,
                               noReplSet : true,
                               forgetPort : true,
                               arbiter : true,
                               noJournalPrealloc : true,
                               noJournal : true,
                               binVersion : true,
                               waitForConnect : true }

MongoRunner.toRealPath = function( path, pathOpts ){

    // Replace all $pathOptions with actual values
    pathOpts = pathOpts || {}
    path = path.replace( /\$dataPath/g, MongoRunner.dataPath )
    path = path.replace( /\$dataDir/g, MongoRunner.dataDir )
    for( key in pathOpts ){
        path = path.replace( RegExp( "\\$" + RegExp.escape(key), "g" ), pathOpts[ key ] )
    }

    // Relative path
    // Detect Unix and Windows absolute paths
    // as well as Windows drive letters
    // Also captures Windows UNC paths

    if( ! path.match( /^(\/|\\|[A-Za-z]:)/ ) ){
        if( path != "" && ! path.endsWith( "/" ) )
            path += "/"

        path = MongoRunner.dataPath + path
    }

    return path

}

MongoRunner.toRealDir = function( path, pathOpts ){

    path = MongoRunner.toRealPath( path, pathOpts )

    if( path.endsWith( "/" ) )
        path = path.substring( 0, path.length - 1 )

    return path
}

MongoRunner.toRealFile = MongoRunner.toRealDir;

// Given a test name figures out a directory for that test to use for dump files and makes sure
// that directory exists and is empty.
MongoRunner.getAndPrepareDumpDirectory = function(testName) {
    var dir = MongoRunner.dataPath + testName + "_external/";
    resetDbpath(dir);
    return dir;
}

MongoRunner.nextOpenPort = function(){
    var i = 0;
    while( MongoRunner.usedPortMap[ "" + ( 27000 + i ) ] ) i++;
    MongoRunner.usedPortMap[ "" + ( 27000 + i ) ] = true
    return 27000 + i

}

/**
 * Returns an iterator object which yields successive versions on toString(), starting from a
 * random initial position, from an array of versions.
 *
 * If passed a single version string or an already-existing version iterator, just returns the
 * object itself, since it will yield correctly on toString()
 *
 * @param {Array.<String>}|{String}|{versionIterator}
 */
MongoRunner.versionIterator = function( arr, isRandom ){

    // If this isn't an array of versions, or is already an iterator, just use it
    if( typeof arr == "string" ) return arr
    if( arr.isVersionIterator ) return arr

    if (isRandom == undefined) isRandom = false;

    // Starting pos
    var i = isRandom ? parseInt( Random.rand() * arr.length ) : 0;

    return new MongoRunner.versionIterator.iterator(i, arr);
}

MongoRunner.versionIterator.iterator = function(i, arr) {
    this.toString = function() {
        i = ( i + 1 ) % arr.length
        print( "Returning next version : " + i +
               " (" + arr[i] + ") from " + tojson( arr ) + "..." );
        return arr[ i ]
    }
    this.isVersionIterator = true;
}

/**
 * Converts the args object by pairing all keys with their value and appending
 * dash-dash (--) to the keys. The only exception to this rule are keys that
 * are defined in MongoRunner.logicalOptions, of which they will be ignored.
 *
 * @param {string} binaryName
 * @param {Object} args
 *
 * @return {Array.<String>} an array of parameter strings that can be passed
 *   to the binary.
 */
MongoRunner.arrOptions = function( binaryName , args ){

    var fullArgs = [ "" ]

    if ( isObject( args ) || ( args.length == 1 && isObject( args[0] ) ) ){

        var o = isObject( args ) ? args : args[0]

        // If we've specified a particular binary version, use that
        if( o.binVersion && o.binVersion != "latest" && o.binVersion != "" )
            binaryName += "-" + o.binVersion

        // Manage legacy options
        var isValidOptionForBinary = function( option, value ){

            if( ! o.binVersion ) return true

            // Version 1.x options
            if( o.binVersion.startsWith( "1." ) ){

                return [ "nopreallocj" ].indexOf( option ) < 0
            }

            return true
        }

        for ( var k in o ){

            // Make sure our logical option should be added to the array of options
            if( ! o.hasOwnProperty( k ) ||
                  k in MongoRunner.logicalOptions ||
                ! isValidOptionForBinary( k, o[k] ) ) continue

            if ( ( k == "v" || k == "verbose" ) && isNumber( o[k] ) ){
                var n = o[k]
                if ( n > 0 ){
                    if ( n > 10 ) n = 10
                    var temp = "-"
                    while ( n-- > 0 ) temp += "v"
                    fullArgs.push( temp )
                }
            }
            else {
                if( o[k] == undefined || o[k] == null ) continue
                fullArgs.push( "--" + k )
                if ( o[k] != "" )
                    fullArgs.push( "" + o[k] )
            }
        }
    }
    else {
        for ( var i=0; i<args.length; i++ )
            fullArgs.push( args[i] )
    }

    fullArgs[ 0 ] = binaryName
    return fullArgs
}

MongoRunner.arrToOpts = function( arr ){

    var opts = {}
    for( var i = 1; i < arr.length; i++ ){
        if( arr[i].startsWith( "-" ) ){
            var opt = arr[i].replace( /^-/, "" ).replace( /^-/, "" )

            if( arr.length > i + 1 && ! ("" + arr[ i + 1 ]).startsWith( "-" ) ){
                opts[ opt ] = arr[ i + 1 ]
                i++
            }
            else{
                opts[ opt ] = ""
            }

            if( opt.replace( /v/g, "" ) == "" ){
                opts[ "verbose" ] = opt.length
            }
        }
    }

    return opts
}

/**
 * Returns a new argArray with any test-specific arguments added.
 */
appendSetParameterArgs = function (argArray) {
    var programName = argArray[0];
    if (programName.endsWith('mongod') || programName.endsWith('mongos')) {
        if (jsTest.options().enableTestCommands) {
            argArray.push.apply(argArray, ['--setParameter', "enableTestCommands=1"]);
        }
        if (jsTest.options().authMechanism && jsTest.options().authMechanism != "MONGODB-CR") {
            var hasAuthMechs = false;
            for (i in argArray) {
                if (typeof argArray[i] === 'string' &&
                    argArray[i].indexOf('authenticationMechanisms') != -1) {
                    hasAuthMechs = true;
                    break;
                }
            }
            if (!hasAuthMechs) {
                argArray.push.apply(argArray,
                                    ['--setParameter',
                                     "authenticationMechanisms=" + jsTest.options().authMechanism]);
            }
        }
        if (jsTest.options().auth) {
            argArray.push.apply(argArray, ['--setParameter', "enableLocalhostAuthBypass=false"]);
        }

        // mongos only options
        if (programName.endsWith('mongos')) {
            // apply setParameters for mongos
            if (jsTest.options().setParametersMongos) {
                var params = jsTest.options().setParametersMongos.split(",");
                if (params && params.length > 0) {
                    params.forEach(function(p) {
                        if (p) argArray.push.apply(argArray, ['--setParameter', p])
                    });
                }
            }
        }
        // mongod only options
        else if (programName.endsWith('mongod')) {
            // apply setParameters for mongod
            if (jsTest.options().setParameters) {
                var params = jsTest.options().setParameters.split(",");
                if (params && params.length > 0) {
                    params.forEach(function(p) {
                        if (p) argArray.push.apply(argArray, ['--setParameter', p])
                    });
                }
            }
        }
    }
    return argArray;
};


}());
