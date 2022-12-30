package mle.jobs;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class calcNr4Partition {
    
    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private static LongWritable one_count = new LongWritable(1);
        private int partition;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            partition = context.getConfiguration().getBoolean("partition", true) ? 0 : 1;
        }
        

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length == 3) {
                context.write(new LongWritable(Long.parseLong(line[partition + 1])), one_count);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        private static LongWritable sum_count = new LongWritable(0);

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) { // values is the list of 1s for each key (R)
                sum += val.get();
            }
            sum_count.set(sum);
            context.write(key, sum_count); // key is R
        }
    }

     public static class PartitionerClass extends Partitioner<LongWritable, LongWritable> {
        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;  
        }
    }
}

