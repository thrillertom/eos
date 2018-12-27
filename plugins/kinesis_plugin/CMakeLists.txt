cmake_minimum_required(VERSION 3.09)

#if(BUILD_KINESIS_PLUGIN)
# Locate the aws sdk for c++ package.
find_package(AWSSDK REQUIRED COMPONENTS kinesis)

file(GLOB HEADERS "include/eosio/mongo_db_plugin/*.hpp")

add_library( kinesis_plugin
  kinesis_plugin.cpp
  ${HEADERS} )

find_library(lz4_location NAMES liblz4.a)
add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${lz4_location})

target_include_directories( kinesis_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

target_link_libraries(
  kinesis_plugin
  chain_plugin
  eosio_chain
  appbase
  fc
  ${AWSSDK_LINK_LIBRARIES}
  lz4
  )

message("mongo_db_plugin not selected and will be omitted.")
