# `zync` Kafka Connection Guide: From Basic Setup to SSL/TLS

## Summary

This guide walks through multiple configurations for connecting `zync` to
Kafka, starting with the default plaintext setup and progressing to SSL/TLS
and mutual TLS (mTLS).

## Environment

The following steps were performed on an AWS EC2 instance running Ubuntu Server 24.04.

## Install Software

```
$ sudo apt update && sudo apt upgrade -y
$ sudo apt install make openjdk-21-jdk -y
$ wget https://github.com/brimdata/zync/releases/download/v0.11.0/zync-v0.11.0.linux-amd64.tar.gz && sudo tar -xzf zync-v0.11.0.linux-amd64.tar.gz -C /usr/local/bin
$ wget https://dlcdn.apache.org/kafka/4.1.1/kafka_2.13-4.1.1.tgz && tar -xzf kafka_2.13-4.1.1.tgz
```

## Start Kafka With Default Plaintext Config

These are based on steps from the
[Apache Kafka Quickstart](https://kafka.apache.org/quickstart).

```
$ cd ~/kafka_2.13-4.1.1
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
$ bin/kafka-server-start.sh config/server.properties
```

## Plaintext `zync` Config

Start with a simple `zync` config that connects to Kafka via its default
"plaintext" protocol.

```
$ mkdir ~/.zync
$ cat > ~/.zync/kafka.json << EOF
{
  "bootstrap_servers": "127.0.0.1:9092",
  "security_protocol": "PLAINTEXT",
  "sasl_mechanisms": "PLAIN"
}
EOF
$ echo {} > ~/.zync/schema_registry.json
```

## Smoke Test

Create a Kafka topic.

```
$ cd ~/kafka_2.13-4.1.1
$ bin/kafka-topics.sh --create --topic MyTopic --partitions 1 --bootstrap-server localhost:9092
```

Pass data between `zync` and Kafka.

```
$ echo '{s:"hello,world"}' | zync produce -topic MyTopic -
producing messages to topic "MyTopic"...
waiting for Kafka flush...
1 messages produced to topic "MyTopic"

$ zync consume -topic MyTopic
{key:null,value:{s:"hello,world"}}
```

## Generate Test Certificates for Kafka SSL

We'll use a self-signed Certificate Authority (CA) for test purposes.

```
$ mkdir -p ~/kafka-ssl-test && cd ~/kafka-ssl-test
$ openssl genrsa -out ca-key.pem 2048
$ openssl req -new -x509 -key ca-key.pem -out ca-cert.pem -days 365 -subj "/C=US/ST=CA/L=San Francisco/O=Test/OU=Test/CN=TestCA"
$ openssl genrsa -out broker-key.pem 2048
$ openssl req -new -key broker-key.pem -out broker.csr -subj "/C=US/ST=CA/L=San Francisco/O=Test/OU=Test/CN=localhost"
$ openssl x509 -req -in broker.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out broker-cert.pem -days 365 -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")
$ openssl genrsa -out client-key.pem 2048
$ openssl req -new -key client-key.pem -out client.csr -subj "/C=US/ST=CA/L=San Francisco/O=Test/OU=Test/CN=zync-client"
$ openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -days 365
$ openssl pkcs12 -export -in broker-cert.pem -inkey broker-key.pem -out broker.p12 -name localhost -password pass:brokerpass
$ keytool -importkeystore -deststorepass brokerpass -destkeystore broker.keystore.jks -srckeystore broker.p12 -srcstoretype PKCS12 -srcstorepass brokerpass -alias localhost
$ keytool -import -file ca-cert.pem -alias CARoot -keystore broker.truststore.jks -storepass brokerpass -noprompt
```

## Configure Kafka for SSL/TLS

Stop the plaintext Kafka server (Ctrl+C) and restart it with SSL enabled.

```
$ cd ~/kafka_2.13-4.1.1
$ cp config/server.properties config/server.properties.ssl
$ cat >> config/server.properties.ssl << EOF
  # Listeners (CONTROLLER stays on 9093, SSL on 9094)
  listeners=SSL://localhost:9094,CONTROLLER://localhost:9093
  advertised.listeners=SSL://localhost:9094
  controller.listener.names=CONTROLLER
  inter.broker.listener.name=SSL

  # SSL Configuration
  ssl.keystore.location=/home/ubuntu/kafka-ssl-test/broker.keystore.jks
  ssl.keystore.password=brokerpass
  ssl.key.password=brokerpass

  ssl.truststore.location=/home/ubuntu/kafka-ssl-test/broker.truststore.jks
  ssl.truststore.password=brokerpass

  ssl.client.auth=none

  ssl.protocol=TLSv1.2
  ssl.enabled.protocols=TLSv1.2,TLSv1.3
EOF
$ bin/kafka-server-start.sh config/server.properties.ssl
```

## SSL `zync` Config/Test

If repeated, the `zync produce` from our [smoke test](#smoke-test) will now
hang because `zync` is trying to connect with plaintext to an SSL-only port,
so we update the `zync` config to use SSL.

```
$ cat > ~/.zync/kafka.json << EOF
{
  "bootstrap_servers": "localhost:9094",
  "security_protocol": "SSL",
  "tls_server_ca_cert_file": "/home/ubuntu/kafka-ssl-test/ca-cert.pem"
}
EOF
```

Now the smoke test will succeed.

## Configure Kafka for Mutual TLS (mTLS)

Stop the SSL Kafka server (Ctrl+C) and restart it with client certificate
authentication required.

```
$ cd ~/kafka_2.13-4.1.1
$ sed 's/ssl.client.auth=none/ssl.client.auth=required/' config/server.properties.ssl > config/server.properties.mtls
$ bin/kafka-server-start.sh config/server.properties.mtls
```

## Mutual TLS `zync` Config/Test

If repeated, the `zync produce` from our [smoke test](#smoke-test) will now
hang because the Kafka broker requires client certificate authentication, so
we update the `zync` config to include client certificates.

```
$ cat > ~/.zync/kafka.json << EOF
{
  "bootstrap_servers": "localhost:9094",
  "security_protocol": "SSL",
  "tls_server_ca_cert_file": "/home/ubuntu/kafka-ssl-test/ca-cert.pem",
  "tls_client_cert_file": "/home/ubuntu/kafka-ssl-test/client-cert.pem",
  "tls_client_key_file": "/home/ubuntu/kafka-ssl-test/client-key.pem"
}
EOF
```

Now the smoke test will succeed.

## Troubleshooting

If `zync produce` hangs, verify:
- Kafka is running and listening on the expected port
- Your `~/.zync/kafka.json` `bootstrap_servers` matches your Kafka listener port
- The certificate paths in your config are correct and accessible
