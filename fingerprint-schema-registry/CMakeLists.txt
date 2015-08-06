project(fingerprint-schema-registry)

SET(SRCS
fingerprint-schema-registry.cpp
)

add_executable(fingerprint-schema-registry ${SRCS})
target_link_libraries(fingerprint-schema-registry csi-avro-utils csi-http csi-http-parser ${EXT_LIBS})
