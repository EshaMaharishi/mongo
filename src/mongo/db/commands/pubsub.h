// pubsub.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
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
#include "mongo/base/init.h"
#include <zmq.hpp>

namespace mongo {

    extern zmq::context_t zmq_context;
    extern zmq::socket_t int_pub_socket; // publishes to clients
    extern zmq::socket_t ext_pub_socket; // publishes to the other node(s) in the group
    extern zmq::socket_t ext_sub_socket; // subscribes to the other node(s) in the group

    // to be included in all files using the client's sub sockets
    extern const char *const INT_PUBSUB_ENDPOINT;

	void proxy(zmq::socket_t *subscriber, zmq::socket_t *publisher);

    class PubSubData {

    public:
        static OID addSubscription( const char *channel );
        static zmq::socket_t * getSubscription( OID oid );
        static int removeSubscription( OID oid );

    private:
        static std::map<OID, zmq::socket_t *> open_subs;
    };

}  // namespace mongo
