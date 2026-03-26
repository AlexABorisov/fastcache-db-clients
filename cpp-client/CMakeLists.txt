cmake_minimum_required(VERSION 3.10.0)
project(cache_grpc VERSION 0.1.0 LANGUAGES C CXX)
file(GLOB CLIENT src/cache-client.cxx)
file(GLOB COMPRESSION src/compression.cxx)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -O3 -msse4.2")

find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

if(Protobuf_FOUND OR PROTOBUF_FOUND)
    if(TARGET protobuf::libprotobuf)
        set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
    else()
        set(_PROTOBUF_LIBPROTOBUF ${PROTOBUF_LIBRARIES})
        include_directories(${PROTOBUF_INCLUDE_DIRS})
    endif()
    if(TARGET protobuf::protoc)
        set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
    else()
        set(_PROTOBUF_PROTOC ${PROTOBUF_PROTOC_EXECUTABLE})
    endif()
else()
    message(WARNING "Failed to locate libprotobuf and protoc!")
endif()

# gRPC
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")


# gRPC C++ plugin
find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
# get_target_property(gRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin
#     IMPORTED_LOCATION_RELEASE)
message(STATUS "Using gRPC_CPP_PLUGIN_EXECUTABLE ${gRPC_CPP_PLUGIN_EXECUTABLE}")

# Proto file
get_filename_component(hw_proto "proto/cache.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

message(STATUS "Using proto description ${hw_proto}")

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/cache.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/cache.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/cache.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/cache.grpc.pb.h")
message(STATUS "Env ${hw_grpc_srcs} ${hw_grpc_hdrs}")

add_custom_command(
        OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"  --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${gRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
        DEPENDS "${hw_proto}")

# Generated include directory
include_directories("${CMAKE_CURRENT_BINARY_DIR}" ${PROJECT_SOURCE_DIR}/include ${cache_types_INCLUDE_DIRS})


add_library(${PROJECT_NAME} SHARED ${hw_proto_srcs}
        ${hw_proto_hdrs}
        ${hw_grpc_srcs}
        ${hw_grpc_hdrs}
        ${CLIENT}
        ${COMPRESSION}
)

target_link_libraries( ${PROJECT_NAME} ${_PROTOBUF_LIBPROTOBUF} gRPC::grpc++)
target_link_libraries(ZLIB::ZLIB ${LIBDEFLATE_LIBRARIES})


target_include_directories( ${PROJECT_NAME}
        PUBLIC ${PROJECT_SOURCE_DIR}/include
)

set(${PROJECT_NAME}_INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/include ${CMAKE_CURRENT_BINARY_DIR}
        CACHE INTERNAL "${PROJECT_NAME}: Include Directories" FORCE)


