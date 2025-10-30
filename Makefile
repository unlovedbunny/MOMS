CC = gcc
CFLAGS = -Wall -g `pkg-config --cflags rdkafka`
LDFLAGS = `pkg-config --libs rdkafka`
PROD = producer
CONS = consumer

all: $(PROD) $(CONS)

producer: producer.c
	$(CC) $(CFLAGS) -o $(PROD) producer.c $(LDFLAGS)

consumer: consumer.c
	$(CC) $(CFLAGS) -o $(CONS) consumer.c $(LDFLAGS)

clean:
	rm -f $(PROD) $(CONS)

