package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.sources.WebSocketFunction;

public class WindowWordCount {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new WebSocketFunction("wss://api.tiingo.com/fx", "{\n" +
				"   \"eventName\":\"subscribe\",\n" +
				"   \"authorization\":\"be52bbb227b54f3f631930ffd45d73c44a756e15\",\n" +
				"   \"eventData\":{\n" +
				"      \"thresholdLevel\":5,\n" +
				"      \"tickers\": [ \"eurusd\"]\n" +
				"\n" +
				"   }\n" +
				"}")).print();
		env.execute();

	}

}