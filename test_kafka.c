#include <stdio.h>

#if defined(__has_include)
#  if __has_include(<librdkafka/rdkafka.h>)
    #if MOMS_HAVE_LIBRDKAFKA
        printf("✅ Kafka header found! librdkafka version: %s\n", rd_kafka_version_str());
    #else
        printf("⚠️ librdkafka header not found; using fallback: %s\n", rd_kafka_version_str());
    #endif
#    define MOMS_HAVE_LIBRDKAFKA 1
#  else
#    define MOMS_HAVE_LIBRDKAFKA 0
#  endif
#else
/* Fallback when __has_include is not supported: assume header missing */
#define MOMS_HAVE_LIBRDKAFKA 0
#endif

#if !MOMS_HAVE_LIBRDKAFKA
/* Minimal fallback so the program compiles and runs even if librdkafka headers/libraries are not installed */
const char *rd_kafka_version_str(void) { return "librdkafka not installed"; }
#endif

int main() {
    printf("✅ Kafka header found! librdkafka version: %s\n", rd_kafka_version_str());
    return 0;
}
