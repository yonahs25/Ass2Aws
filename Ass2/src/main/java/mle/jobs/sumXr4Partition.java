package mle.jobs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Stack;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



public class sumXr4Partition {


    // map the Partitioned file
    public static class MapperClassPartition extends Mapper<LongWritable, Text, Text, Text> {
        private int partition;
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            partition = context.getConfiguration().getInt("p", -1);
        }

        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (partition != -1){
                long summed = Long.parseLong(split[1]) + Long.parseLong(split[2]);
                context.write(new Text(String.valueOf(summed)), value);
                // context.write(new Text(split[partition]) , value);
            }
        }

        @Override
        public void cleanup (Context context) throws IOException, InterruptedException {
        }
    }


    // map the Nr
    public static class MapperClass4Xr extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
        }
        @Override
        public void cleanup (Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (split.length != 2) return;
            context.write(new Text(split[0]) , new Text(split[1]));
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        private boolean found_wanted_Xr = false;
        private long current_Xr;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }
        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            current_Xr = 0;
            Stack<String> waiting_trigrams = new Stack<String>(); // stack of trigrams

            for (Text val : values) {
                String[] split = val.toString().split("\t");
                if (split.length == 1) {
                    current_Xr = Long.parseLong(split[0]);
                    found_wanted_Xr = true;
                } else {
                    if (split.length != 3) return;
                    if (found_wanted_Xr){
                        context.write(new Text(split[0]), new Text(Long.toString(current_Xr)));
                    } else {
                        waiting_trigrams.push(split[0]);
                    }
                }
            }
            while (!waiting_trigrams.isEmpty()){
                context.write(new Text(waiting_trigrams.pop()), new Text(Long.toString(current_Xr)));
            }
            found_wanted_Xr = false;
            current_Xr = 0;
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
