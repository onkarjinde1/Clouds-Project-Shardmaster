#ifndef SHARDING_SHARDKV_MANAGER_H
#define SHARDING_SHARDKV_MANAGER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include "../common/common.h"
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <fstream>

#include "../build/shardkv.grpc.pb.h"
#include "../build/shardmaster.grpc.pb.h"


class PingInterval {
    std::chrono::time_point<std::chrono::system_clock> time;
public:
    std::uint64_t GetPingInterval(){
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()-time.time_since_epoch()).count();
    };
    void Push(std::chrono::time_point<std::chrono::system_clock> t){
        time = t;
    }
};

class ShardkvManager : public Shardkv::Service {
  using Empty = google::protobuf::Empty;

 public:
  explicit ShardkvManager(std::string addr, const std::string& shardmaster_addr)
      : address(std::move(addr)), sm_address(shardmaster_addr) {
      // TODO: Part 3
      // This thread will check for last shardkv server ping and update the view accordingly if needed
      std::thread heartbeatChecker(
              [this]() {
                  std::chrono::milliseconds timespan(1000);
                  while (true) {
                      std::this_thread::sleep_for(timespan);
                  }
              });
      // We detach the thread so we don't have to wait for it to terminate later
      heartbeatChecker.detach();
  };

  // TODO implement these three methods
  ::grpc::Status Get(::grpc::ServerContext* context,
                     const ::GetRequest* request,
                     ::GetResponse* response) override;
  ::grpc::Status Put(::grpc::ServerContext* context,
                     const ::PutRequest* request, Empty* response) override;
  ::grpc::Status Append(::grpc::ServerContext* context,
                        const ::AppendRequest* request,
                        Empty* response) override;
  ::grpc::Status Delete(::grpc::ServerContext* context,
                        const ::DeleteRequest* request,
                        Empty* response) override;
  ::grpc::Status Ping(::grpc::ServerContext* context, const PingRequest* request,
                        ::PingResponse* response) override;

 private:
    // address we're running on (hostname:port)
    const std::string address;

    // shardmaster address
    std::string sm_address;

    // TODO add any fields you want here!
};
#endif  // SHARDING_SHARDKV_MANAGER_H
