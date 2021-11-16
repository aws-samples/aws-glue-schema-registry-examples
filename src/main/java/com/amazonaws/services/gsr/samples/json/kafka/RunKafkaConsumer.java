package com.amazonaws.services.gsr.samples.json.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RunKafkaConsumer {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Please provide command line arguments: configPath topic");
			System.exit(1);
		}
		System.out.println("Hello World!");
		final Properties props = loadConfig(args[0]);
		System.out.println(props);

		final String topic = args[1];

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
		props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
		props.put(AWSSchemaRegistryConstants.DATA_FORMAT, "JSON");
		props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "GsrBlogRegistry");
		props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "GsrBlogSchema");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		Consumer<String, JsonDataWithSchema> consumer = new KafkaConsumer<String, JsonDataWithSchema>(props);
		consumer.subscribe(Arrays.asList(topic));

		try {
			while (true) {
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, JsonDataWithSchema> records = consumer.poll(100);
				for (ConsumerRecord<String, JsonDataWithSchema> record : records) {
					String key = record.key();
					JsonDataWithSchema value = record.value();
					System.out.println("Received message: key = " + key + ", value = " + value);
				}
			}
		} finally {
			consumer.close();
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
