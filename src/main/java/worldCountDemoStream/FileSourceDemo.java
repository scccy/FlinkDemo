package worldCountDemoStream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FileSourceDemo  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        FileSource<String> build = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/word1.txt")
        ).build();

        executionEnvironment.fromSource(build, WatermarkStrategy.noWatermarks(),"fileSource")
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String word : s.split(",")) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                })
//                .keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
//                .sum(1)
                .print();
                ;
        executionEnvironment.execute();
    }
}
