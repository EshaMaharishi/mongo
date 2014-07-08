// pubsub_poll.cpp

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

    class PollCommand : public Command {
    public:
        PollCommand() : Command("poll") {}

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
            help << "{ poll : 1 , _id : ObjectID }";
        }

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {

            // ensure that the sub_id argument is an ObjectID
            uassert(18533,
                    mongoutils::str::stream() << "The sub_id argument to the subscribe " <<  
                        "command must be an ObjectID but was a " << typeName(cmdObj["sub_id"].type()),
                    cmdObj["sub_id"].type() == jstOID );
            BSONElement boid = cmdObj["sub_id"]; 
            OID oid = boid.OID();

            // retrieve the internal subscribe socket for this subscription
            zmq::socket_t *sub_sock = PubSubData::getSubscription( oid );
            uassert(18534,
                    mongoutils::str::stream() << "ObjectID passed as sub_id is not a valid " <<         
                        "id for an open subscription.", 
                    sub_sock != NULL );

            // initialize poll set
            long timeout = cmdObj["timeout"].numberInt();
            zmq::pollitem_t items [] = {
                { *sub_sock, 0, ZMQ_POLLIN, 0 }
            };
            zmq::poll(&items[0], 1, timeout);

            // keep an outbox to demultiplex messages before serializing into BSON             
            std::map<string, BSONArrayBuilder *> outbox;
            zmq::message_t msg;

            // first part of each message is the channel name,
            // second part is the message body (as a document)
            while ( sub_sock->recv(&msg, ZMQ_DONTWAIT)) { 

                string channelName = string((char *)msg.data());

                // if first new message on this channel, add channel to our outbox
                if (outbox.find(channelName) == outbox.end())
                    outbox.insert(std::make_pair(channelName, new BSONArrayBuilder()));
                BSONArrayBuilder *arrayBuilder = outbox.find(channelName)->second;

                // receive and save message body
                msg.rebuild();
                sub_sock->recv(&msg);
                BSONObj messageObject((const char *)msg.data());
                arrayBuilder->append(messageObject);

                msg.rebuild();
            }

            // serialize the outbox into BSON
            BSONObjBuilder b;    
            for (std::map<string, BSONArrayBuilder *>::iterator it = outbox.begin(); 
                 it != outbox.end();
                 ++it) {
                b.append(it->first , it->second->arr());
                delete(it->second);
            }

            result.append( "messages" , b.obj() );
            return true;
        }

    } pollCmd;

}  // namespace mongo
