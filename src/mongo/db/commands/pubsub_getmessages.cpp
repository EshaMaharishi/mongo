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
#include "mongo/db/commands/pubsub.h"

namespace mongo {

    class GetMessagesCommand : public Command {
    public:
        GetMessagesCommand() : Command("getMessages") {}

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
            help << "{ getMessages : 1 , _id : ObjectID }";
        }

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result,
                 bool fromRepl ) {

            // TODO: do input validation

            BSONElement boid = cmdObj["sub_id"]; 
            OID oid = boid.OID();

            // check socket out from global table
            zmq::socket_t *sub_sock = PubSubData::getSubscription( oid );
            if( sub_sock == NULL )
                return false;
            
            std::map<string, BSONArrayBuilder *> messages;

            zmq::message_t msg;
            while (sub_sock->recv(&msg, ZMQ_DONTWAIT)) {

                printf("reached 1\n\n\n\n");

                string channelName = string((char *)msg.data());

                if (messages.find(channelName) == messages.end()) {
                    messages.insert(std::make_pair(channelName, new BSONArrayBuilder()));
                }

                printf("reached 2\n\n\n\n");

                BSONArrayBuilder *arrayBuilder = messages.find(channelName)->second;

                msg.rebuild();

                sub_sock->recv(&msg);

                printf("reached 3\n\n\n\n");

                BSONObj messageObject((const char *)msg.data());
                arrayBuilder->append(messageObject);

                printf("reached 4\n\n\n\n");

                msg.rebuild();
            }

            BSONObjBuilder b;    

            if (messages.size() > 0) {
                cout << "map length: " << messages.size() << endl;
                for (std::map<string, BSONArrayBuilder *>::iterator it = messages.begin(); 
                     it != messages.end();
                     ++it) {
                    b.append(it->first , it->second->arr());
                    delete(it->second);
                }
                printf(">>>> messages found: %s\n", b.obj().jsonString().c_str());
            }

            result.append( "messages" , b.obj() );
            return true;
        }

    } getMessagesCmd;

}  // namespace mongo
