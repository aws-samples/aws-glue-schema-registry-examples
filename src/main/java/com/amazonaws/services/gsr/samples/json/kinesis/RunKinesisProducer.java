package com.amazonaws.services.gsr.samples.json.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.common.Schema;

import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.FileReader;
import java.io.FileNotFoundException;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.List;
import java.util.ArrayList;

import java.nio.ByteBuffer;

public class RunKinesisProducer {
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Please provide command line arguments: stream");
            System.exit(1);
        }

        final String streamName = args[0];
        final String regionName = "us-west-2";
        final String registryName = "GsrBlogRegistry";
        final String schemaName = "GsrBlogSchema";

        final DataFormat dataFormat = DataFormat.JSON;

        AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build();

        AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.builder().build();

        GlueSchemaRegistryConfiguration gsrConfig = new GlueSchemaRegistryConfiguration(regionName);
        gsrConfig.setRegistryName(registryName);
        gsrConfig.setSchemaAutoRegistrationEnabled(true);

        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializerImpl(
                awsCredentialsProvider, gsrConfig);

        GlueSchemaRegistryDataFormatSerializer dataFormatSerializer = new GlueSchemaRegistrySerializerFactory()
                .getInstance(dataFormat, gsrConfig);

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

                byte[] serializedRecord = dataFormatSerializer.serialize(r);
                Schema gsrSchema = new Schema(dataFormatSerializer.getSchemaDefinition(r), dataFormat.name(),
                        schemaName);
                byte[] gsrEncodedBytes = glueSchemaRegistrySerializer.encode(streamName, gsrSchema, serializedRecord);
                ByteBuffer gsrEncodedByteBuffer = ByteBuffer.wrap(gsrEncodedBytes);

                PutRecordRequest putRecordRequest = new PutRecordRequest();

                putRecordRequest.setStreamName(streamName);
                putRecordRequest.setPartitionKey(String.valueOf("message-" + i));
                putRecordRequest.setData(gsrEncodedByteBuffer);

                System.out.println("Putting 1 record into " + streamName);

                kinesisClient.putRecord(putRecordRequest);

                System.out.println("Sent message " + i);
                Thread.sleep(1000L);
            }
            System.out.println("Successfully produced " + genericJsonRecords.size() + " messages to a stream called "
                    + streamName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}