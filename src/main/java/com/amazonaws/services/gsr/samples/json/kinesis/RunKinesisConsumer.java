package com.amazonaws.services.gsr.samples.json.kinesis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import com.amazonaws.services.schemaregistry.common.Schema;

import software.amazon.awssdk.services.glue.model.DataFormat;

import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.ByteBuffer;

public class RunKinesisConsumer {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Please provide command line arguments: stream");
			System.exit(1);
		}

		final String streamName = args[0];
		final String regionName = "us-west-2";

		final DataFormat dataFormat = DataFormat.JSON;

		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build();
		AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();

		GlueSchemaRegistryConfiguration gsrConfig = new GlueSchemaRegistryConfiguration(regionName);

		GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializerImpl(
				awsCredentialsProvider, gsrConfig);

		GlueSchemaRegistryDataFormatDeserializer gsrDataFormatDeserializer = new GlueSchemaRegistryDeserializerFactory()
				.getInstance(dataFormat, gsrConfig);

		ListShardsRequest listShardsRequest = new ListShardsRequest();
		listShardsRequest.setStreamName(streamName);

		try {
			while (true) {
				ListShardsResult listShardsResult = kinesisClient.listShards(listShardsRequest);
				List<Shard> shards = listShardsResult.getShards();
				for (Shard shard : shards) {
					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
					getShardIteratorRequest.setStreamName(streamName);
					getShardIteratorRequest.setShardId(shard.getShardId());
					getShardIteratorRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);

					String shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest).getShardIterator();
					GetRecordsRequest getRecordRequest = new GetRecordsRequest();

					while (!Objects.isNull(shardIterator)) {
						getRecordRequest.setShardIterator(shardIterator);

						GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordRequest);

						shardIterator = getRecordsResult.getNextShardIterator();
						List<Record> records = getRecordsResult.getRecords();
						List<Object> consumerRecords = new ArrayList<>();

						for (Record record : records) {
							ByteBuffer recordAsByteBuffer = record.getData();
							byte[] consumedBytes = recordAsByteBuffer.array();

							Schema gsrSchema = glueSchemaRegistryDeserializer.getSchema(consumedBytes);
							Object decodedRecord = gsrDataFormatDeserializer.deserialize(ByteBuffer.wrap(consumedBytes),
									gsrSchema.getSchemaDefinition());
							consumerRecords.add(decodedRecord);
						}
						if (consumerRecords.size() > 0) {
							System.out.println("Number of Records received: " + consumerRecords.size());
							System.out.println(consumerRecords);
						}
					}
					Thread.sleep(3000L);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
