#include "shardmaster.h"
#include <vector>

/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    std::unique_lock<std::mutex> lock(this->ssm_mtx);

        if(this->ssm.find(request->sever()) != ssm.end()){

            lock.unlock();
                                        
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists");
}

int num_servers = this->ssm.size();
int num_keys = MAX_KEY - MIN_KEY + 1;

num_servers++;

this->ser.push_back(request->server());

std::vector<shard_t> new_vsh;
ssm[request->server()] = new_vsh;

int per_sh = num_keys/num_servers;
int extr = num_keys % num_servers;
int lower = MIN_KEY;
int upper = per_sh-1;


if(per_sh){
    for(auto v:(this->ser)){
        if(extr){
            upper++;
            extr--;
        
        }

        shard_t new_sh;
        new_sh.lower = lower;
        new_sh.upper = upper;
        this->ssm[v].clear();
        this->ssm[v].push_back(new_sh);
        lower = upper+1;
        upper = lower+per_sh-1;

    }
}


lock.unlock();

return ::grpc::Status:OK;

}




/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {

    std::unique_lock<std::mutex> lock(this->ssm_mtx);;
    int num_keys = MAX_KEY - MIN_KEY + 1;

    for(int i = 0; i < request->servers_size(); i++){
        if(this->ssm.find(request->servers(i)) == this->ssm.end()){
            lock.unlock();
        }
    }




    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "the given server does not exists");

    {
        this->ssm.erase(request->servers(i));
        this->ser.erase(find(this->ser.begin(), this->ser.end(), request->servers(i)));
    }

    int num_servers = this->ssm.size();
    int per_sh = num_keys / num_servers;
    int extr = num_keys % num_servers;
    int lower = MIN_KEY;
    int upper = per_sh-1;

    for(auto v : (this->ser)){

        if(extr){
            upper++;
            extr--;
        }
        else if(!per_sh)
            break;

        shard_t new_sh;
        new_sh.lower = lower;
        new_sh.upper = upper;
        this->ssm[v].clear();
        this->ssm[v].push_back(new_sh);
        lower = upper+1;
        upper = lower+per_sh-1;
        }
        lock.unlock();
        return ::grpc::Status::OK;
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {

    std::unique_lock<std::mutex> lock(this->ssm_mtx);
    if(this->ssm.find(request->server()) == this->ssm.end()){
        lock.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "the given server does not exists. Move Error");
    }

    shard_t sh;
    shard_t new_shard;
    shard_t v;
    sh.lower = request->lower();
    sh.upper = request->upper();

    for(auto m:this->ssm){
        std::vector<shard_t> new_shv;

        for (auto v1 : m.second){
            auto ov_st = get_overlap(v1, sh);
            v.lower = v1.lower;
            v.upper = v1.upper;
            if(ov_st == OverlapStatus::overlap_start){
                v.lower = sh.upper + 1;
                new_shv.push_back(v);
            }
            else if(ov_st == OverlapStatus::overlap_end){
                v.upper = sh.lower - 1;
                new_shv.push_back(v);

            }
            else if(ov_st == OverlapStatus::completely_contains){
                new_shard.lower = sh.upper + 1;
                new_shard.upper = v.upper;
                v.upper = sh.lower - 1;
                new_shv.push_back(v);
                new_shv.push_back(new_shard);
            }
            else if (ov_st == OverlapStatus::NO_OVERLAP){
                new_shv.push_back(v);
            }
        }

        this->ssm[m.first] = new_shv;
    }

    this->ssm[request->server()].push_back(sh);
    sortAscendingInterval(this->ssm[request->server()]);
    lock.unlock();
    return ::grpc::Status::OK;
}
  // Hint: Take a look at get_overlap in common.{h, cc}
  // Using the function will save you lots of time and effort!


/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const StaticShardmaster::Empty* request,
                                        ::QueryResponse* response) {
    std::unique_lock<std::mutex> lock(this->ssm_mtx);
    


    for(auto v : (this->ser)){
        auto conf_entry = response->add_config();
        conf_entry->set_server(v);
        for(auto sh : this->ssm[v]){
            auto sh_entry = conf_entry->add_shards();
            sh_entry->set_lower(v1.lower);
            sh_entry->set_upper(v1.upper);
        }
    }
    lock.unlock();
    return ::grpc::Status::OK;
                                        }
