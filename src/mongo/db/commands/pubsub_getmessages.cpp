// getMessages.cpp

/**
*    Copyright (C) 2012 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as getMessagesed by the Free Software Foundation.
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

    class GetMessagesCommand : public Command {
    public:
        GetMessagesCommand() : Command("getMessages"), zmqcontext(1), pub_socket(zmqcontext, ZMQ_PUB) {
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
            help << "{ getMessages : 'collection name' , key : 'a.b' , query : {} }";
        }

        static void bgsubthread(void *context) {
            zmq::context_t *zmqcontext = (zmq::context_t *) context;
            zmq::socket_t sub_socket(*zmqcontext, ZMQ_SUB);
            sub_socket.connect("tcp://localhost:5555");
            sub_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);

            // void *channel = calloc(500, 1);
            // void *message = calloc(500, 1);

            // sub_socket.recv(channel, 500);
            // sub_socket.recv(message, 500);

            while( true ){

                std::map<string, BSONArrayBuilder *> messages;

                zmq::message_t msg;
                while (sub_socket.recv(&msg, ZMQ_DONTWAIT)) {

                    string channelName = string((char *)msg.data());
                    cout << "received a message" << endl;
                    if (messages.find(channelName) == messages.end()) {
                        cout << "inserting field for new channel" << endl;
                        messages.insert(std::make_pair(channelName, new BSONArrayBuilder()));
                    }

                    BSONArrayBuilder *arrayBuilder = messages.find(channelName)->second;

                    msg.rebuild();

                    printf(">>>> receiving second time\n");
                    sub_socket.recv(&msg);

                    BSONObj messageObject((const char *)msg.data());
                    arrayBuilder->append(messageObject);

                    msg.rebuild();

                    // message.append("channel", channelName);

                    // // get the body
                    // invariant(sock.);
                    // invariant(!msg.more());
                    // const auto body = BSONElement(static_cast<const char*>(msg.data()));
                    // invariant(size_t(body.size()) == msg.size());
                    // invariant(body.fieldNameStringData() == "msg");
                    // message.append(body);
                    // msg.rebuild();
                }
                
                // result.append("messages", messages.arr());

                // printf(">>>> subscribe socket received data. channel: %s, message: %s\n", channel, messageObject.jsonString().c_str());

                // free(channel);
                // free(message);

                if( messages.size() > 0 ){
                    printf(">>> iterating on map\n");

                    BSONObjBuilder b;
                    cout << "map length: " << messages.size() << endl;
                    for (std::map<string, BSONArrayBuilder *>::iterator it = messages.begin(); 
                         it != messages.end();
                         ++it) {
                        cout << it->first << endl;
                        cout << it->second << endl;
                        b.append( it->first , it->second->arr() );
                        // delete(it->second);
                    }

                    printf(">>>> messages received: %s\n", b.obj().jsonString().c_str());
                    break;
                }
            }
        }

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result,
                 bool fromRepl ) {

            // Timer t;
            // string ns = dbname + '.' + cmdObj.firstElement().valuestr();

            // do validation

            boost::thread thr(bgsubthread, &zmqcontext);
            sleep(1);

            string channel = cmdObj["channel"].String();
            BSONObj message = cmdObj["message"].Obj();

            pub_socket.send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE);
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
    } getMessagesCmd;

}  // namespace mongo
