#include <grpcpp/grpcpp.h>

#include "shardkv_manager.h"

#include "../build/shardkv.grpc.pb.h"
/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
                                    std::unique_lock<std::mutex> lock(this->skv_mtx);;

                                    auto channel = ::grpc::CreateChannel(this->shardKV_address, ::grpc::InsecureChannelCredentials());
                                    auto kvStub = ::Shardkv::NewStub(channel);
                                    ::grpc::ClientContext cc;

                                    auto status = kvStub->get(&cc, *request, response);

                                    if (status.ok()){
                                        std::cout <<"Shardmanager got the response from the server" << std::endl;

                                    }else{
                                        lock.unlock();
                                        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "This Shardmanager failed");
                                    }
                                    lock.unlock();
                                    return ::grpc::Status::Ok;

}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
                                    ::grpc::ClientContext cc;

                                    auto status = kvStub->Put(&cc, *request, response);

                                    if (status.ok()){
                                        std::cout << "Shardmanager got the Put response from the primary server" << std::endl;
                                    }else{
                                        lock.unlock();
                                        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "This shardmanager failed");
                                    }

                                    lock.unlock();
                                    return ::grpc::Status::OK;
    
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvManager::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
                                        std::unique_lock<std::mutex> lock(this->skv_mtx);
                                        auto channel = ::grpc::CreateChannel(this->shardKV_address, ::grpc::InsecureChaneelCredentials());
                                        auto kvStub = Shardkv::NewStub(channel);
                                        ::grpc::ClientContext cc;

                                        auto status = kvStub->Append(&cc, *request, response);

                                        if (stauts.ok()){
                                            std::cout << "Shardmanager got the Append response from the primary server" << std::endl;
                                        }else{
                                            lock.unlock();
                                            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "this shardmanager failed");
                                        }

                                        lock.unlock();
                                        return ::grpc::Status::OK;

                                     }
/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
                                                  std::unique_lock<std::mutex> lock(this->skv_mtx);
                                                  auto channel = ::grpc::CreateChannel(this->shardKV_address, ::grpc::InsecureChannelCredentials());
                                                  auto kvStub = Shardkv::NewStub(channel);
                                                  ::grpc::ClientContext cc;
    
                                                  auto status = kvStub->Delete(&cc, *request, response);
    
                                                  if (status.ok()){
                                                    std::cout << "Shardmanager got the Delete response from the primary server" << std::endl;
                                                  }else{
                                                    lock.unlock();
                                                    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "This shardmanager failed");
                                                  }

                                                  if(!this->shardKV_backup_address.empty()){
                                                    auto channel = ::grpc::CreateChannel(this->shardKV_backup_address, ::grpc::InsecureChannelCredentials());
                                                    auto kvStub = Shardkv::NewStub(channel);
                                                    ::grpc::ClientContext cc2;
    
                                                    auto status = kvStub->Delete(&cc, *request, response);
    
                                                    if (status.ok()){
                                                        std::cout << "Shardmanager got the Delete response from the backup server" << std::endl;
                                                    }else{
                                                        lock.unlock();
                                                        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "This shardmanager failed");
                                                    }
                                                  }

                                                
    
                                                  lock.unlock();
                                                  return ::grpc::Status::OK;
                                             
}

/**
 * In part 2, this function get address of the server sending the Ping request, who became the primary server to which the
 * shardmanager will forward Get, Put, Append and Delete requests. It answer with the name of the shardmaster containeing
 * the information about the distribution.
 *
 * @param context - you can ignore this
 * @param request A message containing the name of the server sending the request, the number of the view acknowledged
 * @param response The current view and the name of the shardmaster
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Ping(::grpc::ServerContext* context, const PingRequest* request,
                                       ::PingResponse* response){
    std::unique_lock<std::mutex> lock(this->skv_mtx);
    std::string skv_address = request->server();
    std::vector<std::string> new_skv;

    if(this->shardKV_address.empty()){
        this->shardKV_address = skv_address;
        this->last_ack_view = current_view;
        this->current_view++;
        new_skv.push_back(this->shardKV_address);
        new_skv.push_back("");
        response->set_id(this->current_view);
        this->views[this->current_view] = new_skv;
        response->set_primary(skv_address);
        response->set_backup("");

    }else if ((this->shardKV_backup_address.empty()) && (skv_address != this->shardKV_address)) {
        this->shardKV_backup_address = skv_address;
        
        this->current_view++;
        new_skv.push_back(this->shardKV_address);
        new_skv.push_back(skv_address);
        
        this->views[this->current_view] = new_skv;
        response->set_primary(this->shardKV_address);
        response->set_backup(this->shardKV_backup_address);
        response->set_id(this->last_ack_view);
    } else if (this->shardKV_address == skv_address){

        this->last_ack_view = request->viewnumber();
        response->set_primary(this->shardKV_address);
        response->set_backup(this->shardKV_backup_address);
        response->set_id(this->current_view);
    } else if (this->shardKV_backup_address == skv_address){
        std::string primary = this->viewa[this->last_ack_view].at(0);
        std::string backup = this->viewa[this->last_ack_view].at(1);

        response->set_primary(primary);
        response->set_backup(backup);
        response->set_id(this->last_ack_view);
    }else{
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "This shardmanager failed, too many servers to deal with");
    }
    PingInterval p;
    p.Push(std::chrono::high_resolution_clock::now());
    this->pingIntervals[skv_address] = p;

    response->set_shardmaster(this->sm_address);
    
    lock.unlock();
    return ::grpc::Status(::grpc::StatusCode::OK, "Success");
}

