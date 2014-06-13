/**
 * Starts an instance of the specified mongo tool
 *
 * @param {String} binaryName The name of the tool to run
 * @param {Object} opts options to pass to the tool
 *    {
 *      binVersion {string}: version of tool to run
 *    }
 *
 * @see MongoRunner.arrOptions
 */
MongoRunner.runMongoTool = function( binaryName, opts ){
    var opts = opts || {}
    var argsArray = MongoRunner.arrOptions(binaryName, opts)
    return runMongoProgram.apply(null, argsArray);
}

// TODO: see if possible to combine codepath with MongoRunner.run
// which uses _startMongoProgram instead of _runMongoProgram
// this is only called by MongoRunner.runMongoTool
runMongoProgram = function() {
    var args = argumentsToArray( arguments );
    var progName = args[0];

    if ( jsTestOptions().auth ) {
        args = args.slice(1);
        args.unshift( progName,
                      '-u', jsTestOptions().authUser,
                      '-p', jsTestOptions().authPassword,
                      '--authenticationMechanism', DB.prototype._defaultAuthenticationMechanism,
                      '--authenticationDatabase=admin'
                    );
    }

    if (progName == 'mongo' && !_useWriteCommandsDefault()) {
        progName = args[0];
        args = args.slice(1);
        args.unshift(progName, '--useLegacyWriteOps');
    }

    return _runMongoProgram.apply( null, args );
}
