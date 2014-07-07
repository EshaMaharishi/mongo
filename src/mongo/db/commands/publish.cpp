// publish.cpp

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

#include <string>
#include <vector>
#include <boost/thread.hpp>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/instance.h"
#include <zmq.hpp>

namespace mongo {

    class PublishCommand : public Command {
    public:
        PublishCommand() : Command("publish"), zmqcontext(1), pub_socket(zmqcontext, ZMQ_PUB) {
            printf(">>>>>>>>>>>> IN PUBLISH COMMAND CONSTRUCTOR\n");
            pub_socket.bind("tcp://*:5555");
        }

        zmq::context_t zmqcontext;
        zmq::socket_t pub_socket;

        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help( stringstream &help ) const {
            help << "{ publish : 'collection name' , key : 'a.b' , query : {} }";
        }

        static void bgsubthread(void *context) {
            zmq::context_t *zmqcontext = (zmq::context_t *) context;
            zmq::socket_t sub_socket(*zmqcontext, ZMQ_SUB);
            sub_socket.connect("tcp://localhost:5555");
            sub_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);

            void *channel = calloc(500, 1);
            void *message = calloc(500, 1);

            sub_socket.recv(channel, 500);
            sub_socket.recv(message, 500);

            BSONObj messageObject((const char *)message);

            printf(">>>> subscribe socket received data. channel: %s, message: %s\n", channel, messageObject.jsonString().c_str());

            free(channel);
            free(message);
        }

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result,
                 bool fromRepl ) {

            // Timer t;
            // string ns = dbname + '.' + cmdObj.firstElement().valuestr();

            // ensure that the channel is a string
            uassert(18527,
                    mongoutils::str::stream() << "The first (channel) argument to the publish " <<  
                        "command must be a string but was a " << typeName(cmdObj["channel"].type()),
                    cmdObj["channel"].type() == mongo::String);

            // ensure that the message is a document
            if( cmdObj["message"].isNull() == false && cmdObj["query"].eoo() == false ){
             uassert(18528,
                    mongoutils::str::stream() << "The message for the publish command must be a " <<
                        "document but was a " << typeName(cmdObj["message"].type()),
                    cmdObj["message"].type() == mongo::Object);
            }

            boost::thread thr(bgsubthread, &zmqcontext);
            sleep(1);

            string channel = cmdObj["channel"].String();
            BSONObj message = cmdObj["message"].Obj();

            pub_socket.send(channel.data(), channel.size(), ZMQ_SNDMORE);
            pub_socket.send(message.objdata(), message.objsize());

            thr.join();

            {
                BSONObjBuilder b;
                b.append( "channel" , channel );
                b.append( "message" , message );
                result.append( "stats" , b.obj() );
            }

            return true;
        }
    } publishCmd;

}  // namespace mongo
