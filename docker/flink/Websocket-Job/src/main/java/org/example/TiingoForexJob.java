package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.AvroFormatOptions;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.sources.WebSocketFunction;

import java.util.*;

public class TiingoForexJob {

    public static void main(String[] args) throws Exception {
        String url = "wss://api.tiingo.com/crypto";
        String message = "{\n" +
                "   \"eventName\":\"subscribe\",\n" +
                "   \"authorization\":\"be52bbb227b54f3f631930ffd45d73c44a756e15\",\n" +
                "   \"eventData\":{\n" +
                "      \"thresholdLevel\":2\n" +
                //"      \"tickers\": [ \"eurusd\"]\n" +
                "\n" +
                "   }\n" +
                "}";
        String brokers = "broker:29092";
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("http://schema-registry:8081",20);
        String topOfBookSchemaString = client.getLatestSchemaMetadata("top-of-book-valid-value").getSchema();
        Schema topOfBookSchema = new Schema.Parser().parse(topOfBookSchemaString);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final OutputTag<String> outputTag = new OutputTag<String>("invalid-output"){};
        ObjectMapper mapper = new ObjectMapper();
        SingleOutputStreamOperator<GenericRecord> ds = env.addSource(new WebSocketFunction(url, message))
                .map(mapper::readTree)
                .process(new ProcessFunction<JsonNode, GenericRecord>() {
                    @Override
                    public void processElement(JsonNode value, ProcessFunction<JsonNode, GenericRecord>.Context ctx, Collector<GenericRecord> out) throws Exception {
                        try{
                            GenericRecord record = new GenericData.Record(topOfBookSchema);
                            record.put("service",value.get("service").asText());
                            record.put("messageType",value.get("messageType").asText());
                            ArrayList<Object> data = new ArrayList<>();
                            for (JsonNode element : value.get("data")){
                                if (element.isTextual()) {
                                    data.add(element.asText());
                                } else if (element.isDouble()) {
                                    data.add(element.asDouble());
                                }
                            }
                            record.put("data", new GenericData.Array<>(topOfBookSchema.getField("data").schema(), data));
                            out.collect(record);
                        } catch (Exception e) {
                            ctx.output(outputTag, mapper.writeValueAsString(value));
                        }
                    }
                })
                .returns(new GenericRecordAvroTypeInfo(topOfBookSchema));

        KafkaSink<GenericRecord> validSink = KafkaSink.<GenericRecord>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("top-of-book-valid")
                        .setValueSerializationSchema(AvroSerializationSchema.forGeneric(topOfBookSchema, AvroFormatOptions.AvroEncoding.JSON))
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        KafkaSink<String> invalidSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("top-of-book-invalid")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        DataStream<String> sideOutputStream = ds.getSideOutput(outputTag);
        sideOutputStream.print();
        ds.sinkTo(validSink);
        sideOutputStream.sinkTo(invalidSink);
        env.execute();
    }

}