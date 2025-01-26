package worldCountDemoStream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9090);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/word1.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.
                flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {

                    String[] words = s.split(",");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);
        summed.print();
        env.execute();
    }
}
