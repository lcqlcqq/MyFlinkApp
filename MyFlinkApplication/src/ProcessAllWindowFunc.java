import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcessAllWindowFunc {

    private static final int WINDOW_TIMEUNIT = 5;  //窗口时间

    private static String topic = "MyFlinkApp-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        dataStream
                .timeWindowAll(Time.seconds(WINDOW_TIMEUNIT))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        int cnt = 0;
                        int bytes = 0;
                        StringBuilder kafkaData = new StringBuilder();
                        for (String s : iterable) {
                            cnt++;
                            bytes += s.getBytes().length;
                            System.out.println("-> " + s);
                            kafkaData.append(s).append("\n");
                        }
                        String value = String.format("[window]: %s ~ %s, count: %d , size: %d Bytes",
                                time(context.window().getStart()),
                                time(context.window().getEnd()),
                                cnt,
                                bytes);
                        collector.collect(value);
                        KafkaService.produceToKafka(kafkaData.toString(),topic);
                    }
                }).print();
        env.execute("MyFlinkApp-1");

    }

    public static String time(long timeStamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timeStamp));
    }
}