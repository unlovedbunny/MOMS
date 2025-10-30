/* consumer.c
   Consumes messages from topic "trabalho_topic" as part of group "grupo_trabalho".
   Demonstrates rebalance handling and manual commit.
*/
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
const char *brokers = "kafka:9092";

static volatile sig_atomic_t run = 1;
static rd_kafka_t *rk_consumer = NULL;

static void sigterm(int sig) {
    run = 0;
}

/* Rebalance callback */
static void rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        printf("%% Assigned partitions:\n");
        for (int i = 0; i < partitions->cnt; i++) {
    printf("  %s [%d] offset %lld\n",
           partitions->elems[i].topic,
           partitions->elems[i].partition,
           (long long)partitions->elems[i].offset);
}
        rd_kafka_assign(rk, partitions);
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        printf("%% Revoked partitions:\n");
        for (int i = 0; i < partitions->cnt; i++) {
    printf("  %s [%d] offset %lld\n",
           partitions->elems[i].topic,
           partitions->elems[i].partition,
           (long long)partitions->elems[i].offset);
}

        rd_kafka_assign(rk, NULL);
    } else {
        fprintf(stderr, "%% Rebalance error: %s\n", rd_kafka_err2str(err));
        rd_kafka_assign(rk, NULL);
    }
}

int main(int argc, char **argv) {
    const char *brokers = "localhost:9092";
    const char *group_id = "grupo_trabalho";
    const char *topic = "trabalho_topic";
    rd_kafka_conf_t *conf;
    rd_kafka_topic_partition_list_t *topics;
    char errstr[512];

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    conf = rd_kafka_conf_new();

    rd_kafka_conf_set(conf, "bootstrap.servers", brokers, NULL, 0);
    rd_kafka_conf_set(conf, "group.id", group_id, NULL, 0);
    rd_kafka_conf_set(conf, "enable.auto.commit", "true", NULL, 0); /* auto commit */

    /* Set rebalance callback */
    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

    rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk_consumer) {
        fprintf(stderr, "%% Failed to create consumer: %s\n", errstr);
        return 1;
    }

    /* Subscribe to topic */
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
    if (rd_kafka_subscribe(rk_consumer, topics) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "%% Failed to subscribe\n");
        return 1;
    }
    rd_kafka_topic_partition_list_destroy(topics);

    printf("Consumer started. Waiting for messages...\n");

    while (run) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 1000);
        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                /* End of partition event */
            } else {
                fprintf(stderr, "%% Error: %s\n", rd_kafka_message_errstr(rkmessage));
            }
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        printf("%% Received message (topic %s [partition %d] offset %lld): %.*s\n",
               rd_kafka_topic_name(rkmessage->rkt),
               rkmessage->partition,
               (long long)rkmessage->offset,
               (int)rkmessage->len, (char *)rkmessage->payload);

        /* Simulate processing */
        /* ... aqui você pode processar e gravar em BD, etc. ... */

        /* Manual commit of the message's offset for at-least-once */
        rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(offsets, rd_kafka_topic_name(rkmessage->rkt), rkmessage->partition)->offset = rkmessage->offset + 1;
        rd_kafka_commit(rk_consumer, offsets, 0 /* sync */);
        rd_kafka_topic_partition_list_destroy(offsets);

        rd_kafka_message_destroy(rkmessage);
    }

    printf("%% Closing consumer...\n");
    rd_kafka_consumer_close(rk_consumer);
    rd_kafka_destroy(rk_consumer);
    return 0;
}