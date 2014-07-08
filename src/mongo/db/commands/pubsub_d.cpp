// group.cpp

/**
*    Copyright (C) 2012 10gen Inc.
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

#include <zmq.hpp>
#include <boost/thread.hpp>

#include "mongo/db/commands.h"
#include "mongo/db/server_options.h"
#include "mongo/base/init.h"
#include "mongo/db/commands/pubsub.h"

namespace mongo {
	namespace {
		void proxy(zmq::socket_t subscriber, zmq::socket_t publisher) {
			zmq::proxy(subscriber, publisher, NULL);
		}

	    MONGO_INITIALIZER_WITH_PREREQUISITES(SetupPubSubSockets, MONGO_NO_PREREQUISITES)
	                                        (InitializerContext* context) {

	    	const int port = serverGlobalParams.port;
	        const std::string EXT_PUB_ENDPOINT = str::stream() << "tcp://*:" << (port + 2000);
	        const std::string SELF_SUB_ENDPOINT = str::stream() << "tcp://localhost:" << (port + 2000);
	        // send endpoint to config server here if necessary?
	    	ext_pub_socket.bind(EXT_PUB_ENDPOINT.c_str());

	        zmq::socket_t ext_sub_socket(zmq_context, ZMQ_SUB);
	        ext_sub_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
	        ext_sub_socket.connect(SELF_SUB_ENDPOINT.c_str());
	        // connect to all other mongod's in replSet here

	        zmq::socket_t int_pub_socket(zmq_context, ZMQ_PUB);
	        int_pub_socket.bind(INT_PUBSUB_ENDPOINT);

	        // boost::thread(proxy, ext_sub_socket, int_pub_socket);

	        return Status::OK();
	    }
	}
}