/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
//
#include <stdlib.h>
#include <eosio/kinesis_plugin/kinesis_producer.hpp>
#include <eosio/kinesis_plugin/kinesis_plugin.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>
#include <string>

#include <lz4frame.h>

namespace fc { class variant; }

namespace eosio {

  using chain::account_name;
  using chain::action_name;
  using chain::block_id_type;
  using chain::permission_name;
  using chain::transaction;
  using chain::signed_transaction;
  using chain::signed_block;
  using chain::transaction_id_type;
  using chain::packed_transaction;

  enum TransactionType {
                        APPLIED_TRANSACTION = 1,
                        ACCEPTED_TRANSACTION = 2
  };

  static appbase::abstract_plugin& _kinesis_plugin = app().register_plugin<kinesis_plugin>();
  using kinesis_producer_ptr = std::shared_ptr<class kinesis_producer>;

  class kinesis_plugin_impl {
  public:
    kinesis_plugin_impl();

    ~kinesis_plugin_impl();

    fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
    fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
    fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
    fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;
    chain_plugin *chain_plug;

    void consume_blocks();

    void accepted_block(const chain::block_state_ptr &);

    void applied_irreversible_block(const chain::block_state_ptr &);

    void accepted_transaction(const chain::transaction_metadata_ptr &);

    void applied_transaction(const chain::transaction_trace_ptr &);

    void process_accepted_transaction(const chain::transaction_metadata_ptr &);

    void _process_accepted_transaction(const chain::transaction_metadata_ptr &);

    void process_applied_transaction(const chain::transaction_trace_ptr &);

    void _process_applied_transaction(const chain::transaction_trace_ptr &);

    void process_accepted_block(const chain::block_state_ptr &);

    void _process_accepted_block(const chain::block_state_ptr &);

    void process_irreversible_block(const chain::block_state_ptr &);

    void _process_irreversible_block(const chain::block_state_ptr &);

    void add_action_trace(const chain::action_trace &);

    void init(int thread_number = 32);

    uint32_t start_block_num = 0;
    bool start_block_reached = false;

    size_t queue_size = 10000;
    std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
    std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
    std::deque<chain::block_state_ptr> block_state_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_queue;
    boost::mutex mtx;
    boost::condition_variable condition;
    //boost::thread consume_thread;
    std::vector<std::thread> consume_thread_list;
    boost::atomic<bool> done{false};
    boost::atomic<bool> startup{false};
    fc::optional<chain::chain_id_type> chain_id;
    fc::microseconds abi_serializer_max_time;

    static const account_name newaccount;
    static const account_name setabi;

    static const std::string block_states_col;
    static const std::string blocks_col;
    static const std::string trans_col;
    static const std::string trans_traces_col;
    static const std::string actions_col;
    static const std::string accounts_col;
    kinesis_producer_ptr producer;
  };

  const account_name kinesis_plugin_impl::newaccount = "newaccount";
  const account_name kinesis_plugin_impl::setabi = "setabi";

  const std::string kinesis_plugin_impl::block_states_col = "block_states";
  const std::string kinesis_plugin_impl::blocks_col = "blocks";
  const std::string kinesis_plugin_impl::trans_col = "transactions";
  const std::string kinesis_plugin_impl::trans_traces_col = "transaction_traces";
  const std::string kinesis_plugin_impl::actions_col = "actions";
  const std::string kinesis_plugin_impl::accounts_col = "accounts";

  namespace {
    template<typename Queue, typename Entry>
    void queue(boost::mutex &mtx, boost::condition_variable &condition, Queue &queue, const Entry &e) {
      boost::mutex::scoped_lock lock(mtx);
      queue.emplace_back(e);
      condition.notify_one();
    }

  }

  void kinesis_plugin_impl::accepted_transaction(const chain::transaction_metadata_ptr &t) {
    try {
      queue(mtx, condition, transaction_metadata_queue, t);
    } catch (fc::exception &e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
    } catch (std::exception &e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while accepted_transaction");
    }
  }

  void kinesis_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
    try {
      queue(mtx, condition, transaction_trace_queue, t);
    } catch (fc::exception &e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
    } catch (std::exception &e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while applied_transaction");
    }
  }

  void kinesis_plugin_impl::applied_irreversible_block(const chain::block_state_ptr &bs) {
    try {
      queue(mtx, condition, irreversible_block_state_queue, bs);
    } catch (fc::exception &e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
    } catch (std::exception &e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
    }
  }

  void kinesis_plugin_impl::accepted_block(const chain::block_state_ptr &bs) {
    try {
      queue(mtx, condition, block_state_queue, bs);
    } catch (fc::exception &e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
    } catch (std::exception &e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while accepted_block");
    }
  }

  void kinesis_plugin_impl::consume_blocks() {
    std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
    std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
    std::deque<chain::block_state_ptr> block_state_process_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;

    ilog("kinesis_plugin analysis thread started");
    while (true) {
      size_t transaction_metadata_size, transaction_trace_size, block_state_size, irreversible_block_size;
      try {
        {
          boost::mutex::scoped_lock lock(mtx);
          condition.wait(lock, [&]{return done
                || !block_state_queue.empty() || !irreversible_block_state_queue.empty()
                || !transaction_metadata_queue.empty() || !transaction_trace_queue.empty();});
          // capture for processing
          transaction_metadata_size = transaction_metadata_queue.size();
          if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
          }
          transaction_trace_size = transaction_trace_queue.size();
          if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
          }

          block_state_size = block_state_queue.size();
          if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
          }
          irreversible_block_size = irreversible_block_state_queue.size();
          if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
          }
        }

        // warn if queue size greater than 75%
        if (transaction_metadata_size > (queue_size * 0.75) ||
            transaction_trace_size > (queue_size * 0.75) ||
            block_state_size > (queue_size * 0.75) ||
            irreversible_block_size > (queue_size * 0.75)) {
          wlog("queue size: ${q}", ("q", transaction_metadata_size + transaction_trace_size ));
        } else if (done) {
          ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size));
        }

        // process transactions
        while (!transaction_metadata_process_queue.empty()) {
          const auto &t = transaction_metadata_process_queue.front();
          process_accepted_transaction(t);
          transaction_metadata_process_queue.pop_front();
        }

        while (!transaction_trace_process_queue.empty()) {
          const auto &t = transaction_trace_process_queue.front();
          process_applied_transaction(t);
          transaction_trace_process_queue.pop_front();
        }

        // process blocks
        while (!block_state_process_queue.empty()) {
          const auto &bs = block_state_process_queue.front();
          process_accepted_block(bs);
          block_state_process_queue.pop_front();
        }

        // process irreversible blocks
        while (!irreversible_block_state_process_queue.empty()) {
          const auto &bs = irreversible_block_state_process_queue.front();
          process_irreversible_block(bs);
          irreversible_block_state_process_queue.pop_front();
        }

        if (transaction_metadata_size == 0 &&
            transaction_trace_size == 0 &&
            block_state_size == 0 &&
            irreversible_block_size == 0 &&
            done) {
          break;
        }
      } catch (fc::exception &e) {
        elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
      } catch (std::exception &e) {
        elog("STD Exception while consuming block ${e}", ("e", e.what()));
      } catch (...) {
        elog("Unknown exception while consuming block");
      }
    }
    ilog("kinesis_plugin consume thread shutdown gracefully");
  }


  void kinesis_plugin_impl::process_accepted_transaction(const chain::transaction_metadata_ptr &t) {
    try {
      // always call since we need to capture setabi on accounts even if not storing transactions
      _process_accepted_transaction(t);
    } catch (fc::exception &e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception &e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
    }
  }

  void kinesis_plugin_impl::process_applied_transaction(const chain::transaction_trace_ptr &t) {
    try {
      if (start_block_reached) {
        _process_applied_transaction(t);
      }
    } catch (fc::exception &e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception &e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
    }
  }


  void kinesis_plugin_impl::process_irreversible_block(const chain::block_state_ptr &bs) {
    try {
      if (start_block_reached) {
        _process_irreversible_block(bs);
      }
    } catch (fc::exception &e) {
      elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
    } catch (std::exception &e) {
      elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while processing irreversible block");
    }
  }

  void kinesis_plugin_impl::process_accepted_block(const chain::block_state_ptr &bs) {
    try {
      if (!start_block_reached) {
        if (bs->block_num >= start_block_num) {
          start_block_reached = true;
        }
      }
      if (start_block_reached) {
        _process_accepted_block(bs);
      }
    } catch (fc::exception &e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
    } catch (std::exception &e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
    } catch (...) {
      elog("Unknown exception while processing accepted block trace");
    }
  }

  void kinesis_plugin_impl::_process_accepted_transaction(const chain::transaction_metadata_ptr &t) {
    // Note: accepted transaction do nothing.
    //const auto& trx = t->trx;
    //string trx_json = fc::json::to_string( trx );
    //producer->trx_kinesis_sendmsg(ACCEPTED_TRANSCTION, (unsigned char*)trx_json.c_str(), trx_json.length());
  }

  auto make_resolver(controller& db, const fc::microseconds& max_serialization_time) {
    return [&db, max_serialization_time](const account_name &name) -> optional<abi_serializer> {
      const auto &d = db.db();
      const chain::account_object *code_accnt = d.find<chain::account_object, chain::by_name>(name);
      if (code_accnt != nullptr) {
        abi_def abi;
        if (abi_serializer::to_abi(code_accnt->abi, abi)) {
          return abi_serializer(abi, max_serialization_time);
        }
      }
      return optional<abi_serializer>();
    };
  }

  void kinesis_plugin_impl::add_action_trace(const chain::action_trace& atrace) {
    const chain::base_action_trace& base = atrace; // without inline action traces

    fc::variant pretty_output;
    abi_serializer::to_variant(base,
                               pretty_output,
                               make_resolver(chain_plug->chain(), abi_serializer_max_time),
                               abi_serializer_max_time);
    string json = fc::json::to_string(fc::mutable_variant_object(pretty_output.get_object()));
    ilog("action trace: ${action}", ("action", json));
    producer->kinesis_sendmsg(json);

    for( const auto& iline_atrace : atrace.inline_traces ) {
      add_action_trace(iline_atrace);
    }
  }

  void kinesis_plugin_impl::_process_applied_transaction(const chain::transaction_trace_ptr &t) {
    for (const auto& atrace : t->action_traces) {
      add_action_trace(atrace);
    }
  }

  string compress_block(string const &data) {
    const char* raw_data = data.c_str();
    const size_t raw_data_size = data.length();
    const size_t max_dst_size = LZ4F_compressFrameBound(raw_data_size, NULL);
    char* compressed_data = new char[max_dst_size];
    const size_t compressed_data_size = LZ4F_compressFrame(compressed_data, max_dst_size, raw_data, raw_data_size, NULL);

    if (compressed_data_size < 0)
      elog("A negative result from LZ4_compress_default indicates a failure trying to compress the data. failed with code: ${code}.", ("code", compressed_data_size));
    if (compressed_data_size == 0)
      elog("A result of 0 means compression worked, but was stopped because the destination buffer couldn't hold all the information.");

    ilog("raw data size: ${size}", ("size", data.length()));
    string str(compressed_data, compressed_data_size);
    ilog("max_dst_size: ${size}", ("size", max_dst_size));
    ilog("compressed_data_size: ${size}", ("size", compressed_data_size));
    ilog("result_string_size: ${size}", ("size", str.length()));
    delete[] compressed_data;
    compressed_data = 0;

    return str;
  }

  void kinesis_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {
    //dlog(fc::json::to_string(bs));
    //string accepted_block_json = "{"
    //    "\"block_number\": ""
    //string accepted_block_json = "{\"block_finalized\": false, \"data\": " +
    //    fc::json::to_string(bs) + "}";

    auto block = bs->block;

    fc::variant pretty_output;
    abi_serializer::to_variant(*block, pretty_output, make_resolver(chain_plug->chain(), abi_serializer_max_time), abi_serializer_max_time);
    uint32_t ref_block_prefix = block->id()._hash[1];
    string accepted_block_json = fc::json::to_string(fc::mutable_variant_object(pretty_output.get_object())
                                                     ("id", block->id())
                                                     ("block_num", block->block_num())
                                                     ("ref_block_prefix", ref_block_prefix)
                                                     ("finalized", false));
    //string irreversiable_block_json = "{\"block_finalized\": true, \"data\": " + fc::json::to_string(bs) + "}";
    producer->kinesis_sendmsg(accepted_block_json);

    if (block->block_num() % 1000 == 0) {
      ilog("Accept block " + std::to_string(block->block_num()));
    }
  }

  void kinesis_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs) {
    //dlog(fc::json::to_string(bs));
    //const auto block_num = bs->block_num();
    //const auto block_id_str = bs->id().str();
    //const auto previous_block_id_str = bs->previous.str();
    //const auto transaction_mroot_str = bs->transaction_mroot.str();
    //const auto action_mroot_str = bs->action_mroot.str();
    //const auto timestamp = std::chrono::seconds{bs->timestamp.operator fc::time_point().sec_since_epoch()}.count();
    //const auto num_transactions = (int)bs->transactions.size();
    //const auto producer = bs->producer.to_string();
    //const auto schedule_version = bs->schedule_version;
    //const auto confirmed = bs->confirmed;

    auto block = bs->block;
    fc::variant pretty_output;
    abi_serializer::to_variant(*block, pretty_output, make_resolver(chain_plug->chain(), abi_serializer_max_time), abi_serializer_max_time);
    uint32_t ref_block_prefix = block->id()._hash[1];
    string irreversiable_block_json = fc::json::to_string(fc::mutable_variant_object(pretty_output.get_object())
                                                          ("id", block->id())
                                                          ("block_num", block->block_num())
                                                          ("ref_block_prefix", ref_block_prefix)
                                                          ("finalized", true));
    //string irreversiable_block_json = "{\"block_finalized\": true, \"data\": " + fc::json::to_string(bs) + "}";
    string compressed_block = compress_block(irreversiable_block_json);
    producer->kinesis_sendmsg(compressed_block);

    if (block->block_num() % 1000 == 0) {
      ilog("Final block " + std::to_string(block->block_num()));
    }
  }

  kinesis_plugin_impl::kinesis_plugin_impl()
    :producer(new kinesis_producer) {
  }

  kinesis_plugin_impl::~kinesis_plugin_impl() {
    if (startup) {
      try {
        ilog( "kinesis_db_plugin shutdown in process please be patient this can take some minutes" );
        done = true;
        condition.notify_all();
        //consume_thread.join();
        for (auto& thread : consume_thread_list) {
          if (thread.joinable()) {
            thread.join();
          }
        }
        producer->kinesis_destory();
      } catch( std::exception& e ) {
        elog( "Exception on kinesis_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
    }
  }

  void kinesis_plugin_impl::init(int thread_number) {
    ilog("starting kinesis plugin thread");
    abi_serializer_max_time = fc::seconds(20);
    startup = true;
    for (int i = 0; i < thread_number; i++) {
      consume_thread_list.push_back(std::thread([this]{ consume_blocks();}));
    }
    //consume_thread = boost::thread([this] { consume_blocks(); });
  }

  ////////////
  // kinesis_plugin
  ////////////

  kinesis_plugin::kinesis_plugin()
    : my(new kinesis_plugin_impl) {
  }

  kinesis_plugin::~kinesis_plugin() {
  }

  void kinesis_plugin::set_program_options(options_description &cli, options_description &cfg) {
    cfg.add_options()
      ("aws-region-name", bpo::value<std::string>(),
       "AWS region name in string, e.g. 'ap-northeast-1'")
      ("aws-stream-name", bpo::value<std::string>(),
       "The stream name for AWS kinesis.")
      ("kinesis-block-start", bpo::value<uint32_t>()->default_value(0),
       "If specified then only abi data pushed to kinesis until specified block is reached.")
      ;
  }

  void kinesis_plugin::plugin_initialize(const variables_map &options) {
    std::string aws_region_name;
    std::string aws_stream_name;

    try {
      if (options.count("aws-region-name") != 0) {
        aws_region_name = options.at("aws-region-name").as<std::string>();
      } else {
        aws_region_name = "ap-northeast-1";
      }

      if (options.count("aws-stream-name") != 0) {
        aws_stream_name = options.at("aws-stream-name").as<std::string>();
      } else {
        aws_stream_name = "EOS_Asia_Kinesis";
      }

      my->producer->kinesis_init(aws_stream_name, aws_region_name);
      ilog("initializing kinesis_plugin");

      if ( options.count( "kinesis-block-start" )) {
        my->start_block_num = options.at( "kinesis-block-start" ).as<uint32_t>();
      }
      if ( my->start_block_num == 0 ) {
        my->start_block_reached = true;
      }

      // hook up to signals on controller
      //chain_plugin* chain_plug = app().find_plugiin<chain_plugin>();
      my->chain_plug = app().find_plugin<chain_plugin>();
      EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
      auto &chain = my->chain_plug->chain();
      my->chain_id.emplace(chain.get_chain_id());

      // my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
      //     my->accepted_block(bs);
      // }));

      // my->irreversible_block_connection.emplace(
      //                                           chain.irreversible_block.connect([&](const chain::block_state_ptr &bs) {
      //                                                                              my->applied_irreversible_block(bs);
      //                                                                            }));

      // my->accepted_transaction_connection.emplace(
      //     chain.accepted_transaction.connect([&](const chain::transaction_metadata_ptr &t) {
      //         my->accepted_transaction(t);
      //     }));
      my->applied_transaction_connection.emplace(
          chain.applied_transaction.connect([&](const chain::transaction_trace_ptr &t) {
              my->applied_transaction(t);
          }));
      my->init();
    }

    FC_LOG_AND_RETHROW()
      }

  void kinesis_plugin::plugin_startup() {
  }

  void kinesis_plugin::plugin_shutdown() {
    ilog("Kinesis_plugin shutdown.");
    my->accepted_block_connection.reset();
    my->irreversible_block_connection.reset();
    my->accepted_transaction_connection.reset();
    my->applied_transaction_connection.reset();
    my.reset();
    ilog("Kinesis_plugin shutdown successfully.");
  }
} // namespace eosio
