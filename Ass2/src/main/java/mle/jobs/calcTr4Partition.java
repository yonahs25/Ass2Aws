package mle.jobs;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class calcTr4Partition {

     public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private int direction;

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
            if (c.getConfiguration().get("direction").equals("01")) {
                direction = 0; // 0 -> 1
            } else {
                direction = 1; // 1 -> 0
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            System.out.println(Arrays.toString(line));
            System.out.println("@@@@@@@@@@@@@@@");
            if (direction == 0) { // 0 -> 1
                c.write(new LongWritable(Long.parseLong(line[1])), new LongWritable(Long.parseLong(line[2])));
            } else {        // 1 -> 0
                c.write(new LongWritable(Long.parseLong(line[2])), new LongWritable(Long.parseLong(line[1])));
            }
        }

        @Override
        public void cleanup(Context c) throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context c) throws IOException, InterruptedException {
            long sum = 0; // sum of all values
            for (LongWritable value : values) {
                sum += value.get();
            }
            c.write(key, new LongWritable(sum)); // write the sum of all values
        }
    }

    public static class PartitionerClass extends Partitioner<LongWritable, LongWritable> {
        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions; 
        }
    }
    
}
