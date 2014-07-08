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

#include "mongo/bson/oid.h"
#include "mongo/base/init.h"
#include "mongo/db/commands/pubsub.h"

namespace mongo {
    
    zmq::context_t zmq_context(1);
    zmq::socket_t int_pub_socket(zmq_context, ZMQ_PUB);    
    zmq::socket_t ext_pub_socket(zmq_context, ZMQ_PUB);
    zmq::socket_t ext_sub_socket(zmq_context, ZMQ_SUB);


    void proxy(zmq::socket_t *subscriber, zmq::socket_t *publisher) {
        zmq::proxy(*subscriber, *publisher, NULL);
    }


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
     */

    std::map<OID, zmq::socket_t *> PubSubData::open_subs = std::map<OID, zmq::socket_t *>();

    // TODO: add a scoped lock around the table modification
    OID PubSubData::addSubscription( OID oid, zmq::socket_t *sock_ptr ) { 
        if (PubSubData::open_subs.find(oid) == PubSubData::open_subs.end()) {                             
            std::cout << "inserting new subscription in table" << std::endl;                          
            PubSubData::open_subs.insert( std::make_pair( oid, sock_ptr ) );       
            return oid;
        } else {
            // subscription already exists, return existing OID
            return PubSubData::open_subs.find( oid )->first; 
        } 
    } 

}  // namespace mongo
