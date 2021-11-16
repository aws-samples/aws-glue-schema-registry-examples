package com.amazonaws.services.gsr.samples.json.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.*;
import org.json.JSONObject;
import org.json.JSONTokener;

public class RunKafkaProducer {

	public static void createTopic(final String topic, final Properties cloudConfig) {
		final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
		try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
			adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (final InterruptedException | ExecutionException e) {
			if (!(e.getCause() instanceof TopicExistsException)) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Please provide command line arguments: configPath topic");
			System.exit(1);
		}
		
		final Properties props = loadConfig(args[0]);

		final String topic = args[1];
		createTopic(topic, props);

		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
		props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
		props.put(AWSSchemaRegistryConstants.DATA_FORMAT, "JSON");

		props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
		props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "GsrBlogRegistry");
		props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "GsrBlogSchema");

		Producer<String, JsonDataWithSchema> producer = new KafkaProducer<String, JsonDataWithSchema>(props);

		String schemaString = new JSONObject(new JSONTokener(new FileReader("schema.json"))).toString();

		String payload1String = new JSONObject(new JSONTokener(new FileReader("payload1.json"))).toString();
		String payload2String = new JSONObject(new JSONTokener(new FileReader("payload2.json"))).toString();
		String payload3String = new JSONObject(new JSONTokener(new FileReader("payload3.json"))).toString();

		List<JsonDataWithSchema> genericJsonRecords = new ArrayList<>();

		final JsonDataWithSchema jsonRecord1 = JsonDataWithSchema.builder(schemaString, payload1String).build();
		genericJsonRecords.add(jsonRecord1);

		final JsonDataWithSchema jsonRecord2 = JsonDataWithSchema.builder(schemaString, payload2String).build();
		genericJsonRecords.add(jsonRecord2);

		final JsonDataWithSchema jsonRecord3 = JsonDataWithSchema.builder(schemaString, payload3String).build();
		genericJsonRecords.add(jsonRecord3);

		try {
			for (int i = 0; i < genericJsonRecords.size(); i++) {
				JsonDataWithSchema r = genericJsonRecords.get(i);

				final ProducerRecord<String, JsonDataWithSchema> record;
				record = new ProducerRecord<String, JsonDataWithSchema>(topic, "message-" + i, r);

				producer.send(record);
				System.out.println("Sent message " + i);
				Thread.sleep(1000L);
			}
			System.out.println(
					"Successfully produced " + genericJsonRecords.size() + " messages to a topic called " + topic);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	public static Properties loadConfig(final String configFile) throws IOException {
		if (!Files.exists(Paths.get(configFile))) {
			throw new IOException(configFile + " not found.");
		}
		final Properties cfg = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			cfg.load(inputStream);
		}
		return cfg;
	}
}
