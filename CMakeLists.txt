cmake_minimum_required(VERSION 2.8)
INCLUDE(${CMAKE_SOURCE_DIR}/../csi-build-scripts/CMakeDependencies.txt)

if(WIN32)
set(EXT_LIBS ${CSI_KAFKA_LIBS}) 
endif()

if(__LINUX__)
set(EXT_LIBS ${CSI_KAFKA_LIBS})
endif()

add_subdirectory(fingerprint-schema-registry)
add_subdirectory(kafka-avro)
