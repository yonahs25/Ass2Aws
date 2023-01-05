package mle.jobs;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import mle.trigramComparable;

public class sortResults {

    public static class MapperClass extends Mapper<LongWritable, Text, trigramComparable, IntWritable> {

        @Override
        public void setup(Context context) throws java.io.IOException, InterruptedException {
        }
        @Override
        public void cleanup(Context context) throws java.io.IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            trigramComparable trigram = new trigramComparable(value);
            context.write(trigram, new IntWritable(1)); // write the trigram and a dummy value so that the reducer will get it
        }
    }

    public static class ReducerClass extends Reducer<trigramComparable, IntWritable, trigramComparable, Text> {
        @Override
        public void setup(Context context) throws java.io.IOException, InterruptedException {
        }

        @Override
        public void reduce(trigramComparable key, Iterable<IntWritable> dummyValues, Context context) throws java.io.IOException, InterruptedException {
            for ( IntWritable dummyValue : dummyValues) context.write(key, new Text(""));
        }
    }

    public static class PartitionerClass extends Partitioner<trigramComparable, IntWritable> {
        @Override
        public int getPartition(trigramComparable key, IntWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }
    }
}
