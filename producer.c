/* producer.c
   Produces messages to a topic "trabalho_topic" with a delivery report callback.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <librdkafka/rdkafka.h>

const char *brokers = "kafka:9092";


static volatile sig_atomic_t run = 1;

static void sigterm(int sig) {
    run = 0;
}

/* delivery report callback */
static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err) {
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    } else {
        printf("%% Message delivered to topic %s [partition %d] at offset %ld\n",
               rd_kafka_topic_name(rkmessage->rkt),
               rkmessage->partition,
               (long)rkmessage->offset);
    }
}

int main(int argc, char **argv) {
    const char *brokers = "localhost:9092";
    const char *topic = "trabalho_topic";
    rd_kafka_t *rk;           /* Producer instance handle */
    rd_kafka_conf_t *conf;    /* Temporary configuration object */
    char errstr[512];

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    conf = rd_kafka_conf_new();

    /* Set broker list */
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% rd_kafka_conf_set failed: %s\n", errstr);
        return 1;
    }

    /* Strong delivery: wait for all in-sync replicas */
    rd_kafka_conf_set(conf, "acks", "all", NULL, 0);
    rd_kafka_conf_set(conf, "enable.idempotence", "true", NULL, 0); /* prevent duplicates */
    rd_kafka_conf_set(conf, "retries", "5", NULL, 0);

    /* Delivery report callback */
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    /* Create producer */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create rd_kafka_t: %s\n", errstr);
        return 1;
    }

    printf("Producer started. Type messages and enter to send. Ctrl-C to quit.\n");

    char line[1024];
    while (run && fgets(line, sizeof(line), stdin)) {
        size_t len = strlen(line);
        if (len && line[len-1] == '\n') line[len-1] = '\0';

        /* Produce message (asynchronously) */
        rd_kafka_resp_err_t err = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(topic),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(line, strlen(line)),
            RD_KAFKA_V_END);

        if (err) {
            fprintf(stderr, "%% Failed to produce to topic %s: %s\n", topic, rd_kafka_err2str(err));
        } else {
            /* Poll to serve delivery reports */
            rd_kafka_poll(rk, 0);
        }
    }

    /* Wait for outstanding messages to be delivered. */
    fprintf(stderr, "%% Flushing final messages...\n");
    rd_kafka_flush(rk, 10*1000 /* wait for max 10s */);

    rd_kafka_destroy(rk);
    return 0;
}

