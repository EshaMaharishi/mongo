/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once


#include "mongo/bson/oid.h"
#include <zmq.hpp>

namespace mongo {

    /**
     * In-memory data structures for pubsub.
     * Subscribers can poll for more messages on their subscribed channels, and we
     * keep an in-memory map of the id (cursor) they are polling on to the actual
     * poller instance used to retrieve their messages.
     *
     * The map is wrapped in a class to facilitate clean (multi-threaded) access
     * to the table from subscribe (to add entries), unsubscribe (to remove entries),
     * and getMessages (to access entries) without exposing any locking mechanisms
     * to the user.
     *
     * These are in a header because they need to be accessible by multiple commands.
     */

    class PubSubData {

    public:
        PubSubData() : zmqcontext(1) {}

        // TODO: for now use a lock whenever the zmqcontext is being used
        // LATER: see if we need to use multiple zmqcontexts
        zmq::context_t zmqcontext;        

        // TODO: add a scoped lock around the table modification
        static OID addSubscription( OID oid, zmq::socket_t *sock_ptr ) { 

            if ( open_subs.find(oid) == open_subs.end()) {                             
                cout << "inserting new subscription in table" << endl;                          
                open_subs.insert( std::make_pair( oid, sock_ptr ) );       
                return oid;
            } 

            // subscription already exists, return existing OID
            else{
                return open_subs.find( oid )->first; 
            } 
        } 

        static std::map< OID, zmq::socket_t *> open_subs;

    private:

    };


}  // namespace mongo
