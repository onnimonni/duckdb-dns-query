cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME dns_query)

# DNS query extension uses c-ares library

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/quack_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Find and link c-ares library
find_package(c-ares CONFIG REQUIRED)

target_link_libraries(${EXTENSION_NAME} c-ares::cares)
target_link_libraries(${LOADABLE_EXTENSION_NAME} c-ares::cares)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
