#ifndef KINESIS_PRODUCER_HPP
#define KINESIS_PRODUCER_HPP

#include <aws/core/Aws.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>
#include <aws/core/utils/Outcome.h>

#include <eosio/chain/exceptions.hpp>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace eosio {

class kinesis_producer {
 public:
  kinesis_producer(int commit_size = 100, int thread_size = 32) : kCommitSize(commit_size), kThreadSize(thread_size) { }

  void kinesis_init(const std::string& stream_name, const std::string& region_name) {
    ilog("Kinesis Pusher started with region name: " + region_name + "; Stream Name : " + stream_name);
    Aws::InitAPI(m_options);
    mStreamName = stream_name;
    if (region_name == "ap-northeast-1") {
      mRegionName = Aws::Region::AP_NORTHEAST_1;
    }
    m_counter = 0;
    for (int i = 0; i < kThreadSize; i++) {
      m_thread_pool.push_back(std::thread([this]{kinesis_consumer();}));
    }
    ilog("Kinesis Pusher started successfully.");
  }

  void kinesis_sendmsg(std::string msg) {
    std::lock_guard<std::mutex> lock(m_mux_produce);
    m_produce_queue.push_back(std::move(msg));
    m_cond.notify_one();
  }

  void kinesis_consumer() {
    ilog("Kinesis consumer thread started.");
    Aws::Client::ClientConfiguration clientConfig;
    // TODO(necokeine): change to configurable.
    clientConfig.region = Aws::Region::AP_NORTHEAST_1;
    auto client = Aws::Kinesis::KinesisClient(clientConfig);
    Aws::Kinesis::Model::PutRecordsRequest putRecordsRequest;
    putRecordsRequest.SetStreamName(mStreamName.c_str());

    Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry> putRecordsRequestEntryList;
    while (true) {
      putRecordsRequestEntryList.clear();
      std::deque<std::string> elements = get_msg_list();
      if (m_exit && elements.size() == 0) break;
      if (elements.size() >= 10) {
        ilog("Pushing " + std::to_string(elements.size()) + " blocks.");
      }
      for (const std::string& element : elements) {
        //ilog(element);
        Aws::Kinesis::Model::PutRecordsRequestEntry putRecordsRequestEntry;
        Aws::StringStream pk;
        pk << "pk-" << (m_counter++ % 100);
        putRecordsRequestEntry.SetPartitionKey(pk.str());

        Aws::StringStream data;
        data << element;
        Aws::Utils::ByteBuffer bytes((unsigned char *)data.str().c_str(), data.str().length());
        putRecordsRequestEntry.SetData(bytes);
        putRecordsRequestEntryList.push_back(std::move(putRecordsRequestEntry));
      }
      putRecordsRequest.SetRecords(putRecordsRequestEntryList);
      Aws::Kinesis::Model::PutRecordsOutcome putRecordsResult = client.PutRecords(putRecordsRequest);

      while (putRecordsResult.GetResult().GetFailedRecordCount() > 0) {
        wlog("Some records failed, retrying " + std::to_string(putRecordsResult.GetResult().GetFailedRecordCount()));
        Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry> failedRecordsList;
        Aws::Vector<Aws::Kinesis::Model::PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.GetResult().GetRecords();
        for (unsigned int i = 0; i < putRecordsResultEntryList.size(); i++) {
          Aws::Kinesis::Model::PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList[i];
          Aws::Kinesis::Model::PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList[i];
          if (putRecordsResultEntry.GetErrorCode().length() > 0) {
            wlog(putRecordsResultEntry.GetErrorMessage().c_str());
            failedRecordsList.emplace_back(putRecordRequestEntry);
          }
        }
        putRecordsRequestEntryList = failedRecordsList;
        putRecordsRequest.SetRecords(putRecordsRequestEntryList);
        putRecordsResult = client.PutRecords(putRecordsRequest);
      }
      if (elements.size() >= 10) {
        ilog("Pushed " + std::to_string(elements.size()) + " blocks.");
      }
    }
    ilog("Kinesis producer thread finish.");
  }

  void kinesis_destory() {
    ilog("Kinesis producer shutdown.");
    m_exit = true;
    m_cond.notify_all();
    for (int i = 0; i < m_thread_pool.size(); i++) {
      m_thread_pool[i].join();
    }
    Aws::ShutdownAPI(m_options);
    ilog("Kinesis shutdown gracefully.");
  }

 private:
  const int kCommitSize;
  const int kThreadSize;
  std::string mStreamName;
  const char* mRegionName;

  std::deque<std::string> get_msg_list() {
    std::deque<std::string> result;
    std::unique_lock<std::mutex> lock_consume(m_mux_consume);
    if (m_consume_queue.empty()) {
      std::unique_lock<std::mutex> lock_produce(m_mux_produce);
      m_cond.wait(lock_produce, [&]{return m_exit || !m_produce_queue.empty();});
      std::swap(m_produce_queue, m_consume_queue);
    }
    result.clear();
    if (m_consume_queue.size() <= kCommitSize) {
      std::swap(result, m_consume_queue);
    } else {
      for (int i = 0; i < kCommitSize; i++) {
        result.push_back(std::move(m_consume_queue.front()));
        m_consume_queue.pop_front();
      }
    }
    return result;
  }

  std::mutex m_mux_produce;
  std::mutex m_mux_consume;
  std::deque<std::string> m_consume_queue;
  std::deque<std::string> m_produce_queue;

  std::condition_variable m_cond;

  Aws::SDKOptions m_options;
  std::vector<std::thread> m_thread_pool;
  std::atomic<bool> m_exit;
  std::atomic<uint64_t> m_counter;
};

}  // namespace eosio

#endif
