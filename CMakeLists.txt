cmake_minimum_required(VERSION 3.0)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

option(CRUNCH_COMPILE_STANDALONE "Enables building crunch storage engine outside of mariadb build tree" OFF)

project(crunch)

set(CMAKE_CXX_STANDARD 14)

file(GLOB SOURCE_FILES src/*.cpp crunchdelete.capnp.c++)

IF(CRUNCH_COMPILE_STANDALONE)

SET(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-fno-rtti")

find_package(MySQL REQUIRED)

list(LENGTH MYSQL_INCLUDE_DIRS len)
IF(${len} GREATER 1)
    LIST(GET MYSQL_INCLUDE_DIR 0 MYSQL_INCLUDE_DIR_PRIVATE)
    SET(MYSQL_INCLUDE_DIR_PRIVATE "${MYSQL_INCLUDE_DIR_PRIVATE}/private")
ELSE()
    SET(MYSQL_INCLUDE_DIR_PRIVATE "${MYSQL_INCLUDE_DIR}/private")
ENDIF()
include_directories(${MYSQL_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR_PRIVATE})

add_library(crunch SHARED ${SOURCE_FILES})

target_compile_definitions(crunch PRIVATE DBUG_OFF=1 CRUNCH_COMPILE_STANDALONE=1)

if(${MYSQL_LIB_DIR} STREQUAL "/usr/lib" OR ${MYSQL_LIB_DIR} STREQUAL "/usr/lib64")
    install(TARGETS crunch LIBRARY DESTINATION "${MYSQL_LIB_DIR}/mysql/plugin" COMPONENT libraries)
else()
    install(TARGETS crunch LIBRARY DESTINATION "${MYSQL_LIB_DIR}/plugin" COMPONENT libraries)
endif()

ELSE()
    MYSQL_ADD_PLUGIN(crunch ${SOURCE_FILES} STORAGE_ENGINE MODULE_ONLY RECOMPILE_FOR_EMBEDDED)
ENDIF()


find_package(CapnProto REQUIRED)
include_directories(${CAPNP_INCLUDE_DIRS})
target_link_libraries(crunch ${CAPNP_LIBRARIES})