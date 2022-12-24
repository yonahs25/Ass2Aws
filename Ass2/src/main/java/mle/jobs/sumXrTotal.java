package mle.jobs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//Todo: go over names of classes and methods

public class sumXrTotal {
    public static class MapperClassNrFile extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] split = line.toString().split("\t");
            if (split.length == 2) {
                context.write (new Text(split[0]), new Text(split[1]));
            }
        }


        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text trigram, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (Text value : values){
                sum += Long.parseLong(value.toString());
            }
            context.write(trigram, new Text("" + sum));

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
        }
    }



    
}
