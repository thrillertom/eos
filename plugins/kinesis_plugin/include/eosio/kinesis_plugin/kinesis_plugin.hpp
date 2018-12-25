/**
 *
 *
 */
#ifndef KINESIS_PLUGIN_HPP
#define KINESIS_PLUGIN_HPP

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <eosio/chain/account_object.hpp>
#include <appbase/application.hpp>
#include <memory>
#include <string>

namespace eosio {


/**
 * Provides persistence to MongoDB for:
 * accounts
 * actions
 * block_states
 * blocks
 * transaction_traces
 * transactions
 *
 *   See data dictionary (DB Schema Definition - EOS API) for description of MongoDB schema.
 *
 *   If cmake -DBUILD_kinesis_plugin=true  not specified then this plugin not compiled/included.
 */ 
class kinesis_plugin : public plugin<kinesis_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))

   kinesis_plugin();
   virtual ~kinesis_plugin();

   virtual void set_program_options(options_description& cli, options_description& cfg) override;

   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   std::unique_ptr<class kinesis_plugin_impl> my;
};

}  // namespace eosio

#endif
