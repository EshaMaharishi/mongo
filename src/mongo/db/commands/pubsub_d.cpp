// pubsub_d.cpp

/**
*    Copyright (C) 2012 10gen Inc.
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

#include <zmq.hpp>
#include <boost/thread.hpp>
#include <list>

#include "mongo/db/commands.h"
#include "mongo/db/server_options.h"
#include "mongo/base/init.h"
#include "mongo/db/commands/pubsub.h"
#include "mongo/db/repl/rs.h"

namespace mongo {
	namespace {

	    MONGO_INITIALIZER_WITH_PREREQUISITES(SetupPubSubSockets, MONGO_NO_PREREQUISITES)
	                                        (InitializerContext* context) {

	    	const int port = serverGlobalParams.port;
	        const std::string EXT_SUB_ENDPOINT = str::stream() << "tcp://*:" << (port + 2000);

	        ext_sub_socket.bind(EXT_SUB_ENDPOINT.c_str());
	        ext_sub_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);

	        int_pub_socket.bind(INT_PUBSUB_ENDPOINT);

	        boost::thread internal_proxy(proxy, &ext_sub_socket, &int_pub_socket);

	        return Status::OK();
	    }
	}
}
