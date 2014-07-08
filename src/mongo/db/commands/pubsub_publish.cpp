// pubsub_publish.cpp

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
#include "mongo/db/commands/pubsub.h"

namespace mongo {

    class PublishCommand : public Command {
    public:
        PublishCommand() : Command("publish") {}

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

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl ) {

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

            string channel = cmdObj["channel"].String();
            BSONObj message = cmdObj["message"].Obj();

            uassert(18529, "ZeroMQ failed to publish channel name to pub socket.",
                        ext_pub_socket.send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE) 
                        != (unsigned long)(-1) );
            uassert(18530, "ZeroMQ failed to publish message body to pub socket.",
                        ext_pub_socket.send(message.objdata(), message.objsize()) 
                        != (unsigned long)(-1) ); 

            return true;
        }

    } publishCmd;

}  // namespace mongo
