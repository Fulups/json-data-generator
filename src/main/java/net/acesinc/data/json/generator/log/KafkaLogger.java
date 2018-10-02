/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.data.json.generator.log;

import net.acesinc.data.json.util.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author andrewserff
 */
public class KafkaLogger implements EventLogger {

    private static final Logger log = LogManager.getLogger(KafkaLogger.class);
    public static final String BROKER_SERVER_PROP_NAME = "broker.server";
    public static final String BROKER_PORT_PROP_NAME = "broker.port";
    public static final String KERBEROS_CONF = "kerberos";
    public static final String SASL_CONF = "sasl";

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean sync;
    private final boolean flatten;
    private final Properties props = new Properties();
    private JsonUtils jsonUtils;

    public KafkaLogger(Map<String, Object> props) {
        String brokerHost = (String) props.get(BROKER_SERVER_PROP_NAME);
        Integer brokerPort = (Integer) props.get(BROKER_PORT_PROP_NAME);

        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String bootstrapServerAsString = brokerHost + ":" + brokerPort.toString();

        Map<String, String> advancedConf = null;
        if (props.get(KERBEROS_CONF) != null) {
            advancedConf = (Map<String, String>) props.get(KERBEROS_CONF);
            
            this.props.put("security.protocol", advancedConf.get("kafka.security.protocol"));
            this.props.put("sasl.kerberos.service.name", advancedConf.get("kafka.service.name"));
            System.setProperty("java.security.auth.login.config", advancedConf.get("kafka.jaas.file"));
            System.setProperty("java.security.krb5.conf", advancedConf.get("kerberos.conf.file"));
        } else if (props.get(SASL_CONF) != null) {
            advancedConf = (Map<String, String>) props.get(SASL_CONF);
            
            this.props.put("security.protocol", advancedConf.get("kafka.security.protocol"));
            this.props.put("sasl.mechanism", advancedConf.get("kafka.sasl.mechanism"));
            System.setProperty("java.security.auth.login.config", advancedConf.get("kafka.jaas.file"));
        }

        if (advancedConf != null) {
            if (advancedConf.get("kafka.brokers.servers") != null){
                bootstrapServerAsString = advancedConf.get("kafka.brokers.servers");
            } else {
                bootstrapServerAsString = brokerHost + ":" + brokerPort.toString();
            }
            
            if (advancedConf.get("kafka.client.id") != null) {
                this.props.put(ProducerConfig.CLIENT_ID_CONFIG, advancedConf.get("kafka.client.id"));
            }
            if (advancedConf.get("kafka.compression") != null) {
                // Compression values: none, gzip, snappy, or lz4
                this.props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, advancedConf.get("kafka.compression"));
            }
            if (advancedConf.get("kafka.batch.size") != null) {
                this.props.put(ProducerConfig.BATCH_SIZE_CONFIG, advancedConf.get("kafka.batch.size"));
            }
            if (advancedConf.get("kafka.acks") != null) {
                // Acknowledgment values: all, leader or none
                this.props.put(ProducerConfig.ACKS_CONFIG, advancedConf.get("kafka.acks"));
            }
            if (advancedConf.get("kafka.linger.ms") != null) {
                this.props.put(ProducerConfig.LINGER_MS_CONFIG, advancedConf.get("kafka.linger.ms"));
            }
            if (advancedConf.get("kafka.request.per.connection") != null) {
                this.props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, advancedConf.get("kafka.request.per.connection"));
            }
        }
        
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAsString);
        
        producer = new KafkaProducer<>(this.props);

        this.topic = (String) props.get("topic");
        if (props.get("sync") != null) {
            this.sync = (Boolean) props.get("sync");
        } else {
            this.sync = false;
        }

        if (props.get("flatten") != null) {
            this.flatten = (Boolean) props.get("flatten");
        } else {
            this.flatten = false;
        }

        this.jsonUtils = new JsonUtils();
    }

    @Override
    public void logEvent(String event, Map<String, Object> producerConfig) {
        logEvent(event);
    }

    private void logEvent(String event) {

        String output = event;
        if (flatten) {
            try {
                output = jsonUtils.flattenJson(event);
            } catch (IOException ex) {
                log.error("Error flattening json. Unable to send event [ " + event + " ]", ex);
                return;
            }
        }

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, output);
        if (sync) {
            try {
                producer.send(producerRecord).get();
            } catch (InterruptedException | ExecutionException ex) {
                //got interrupted while waiting
                log.warn("Thread interrupted while waiting for synchronous response from producer", ex);
            }
        } else {
            log.debug("Sending event to Kafka: [ " + output + " ]");
            producer.send(producerRecord);
        }
    }

    @Override
    public void shutdown() {
        producer.close();
    }

}
