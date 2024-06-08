package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowWordCount {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromSequence(1, 200).print();
		env.execute();
	}

}