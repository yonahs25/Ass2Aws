package mle.jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import mle.helperMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import static mle.mleManager.totalN;
import java.util.Random;


// First Job - partitions the 3grams into 2 partitions and calculates the N for each partition (N0 and N1)
// Calculate total N
public class partitionsAndN {

        public static class MapperClass extends Mapper<LongWritable, Text, Text, helperMap> {

            @Override
            public void setup(Context context)  throws IOException, InterruptedException {

            }

            @Override
            public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
                Text threegram = new Text();
                helperMap map = new helperMap();
                String[] split = line.toString().split(",");
                //TODO need to check if 3gram  is valid (from the banned words)
                LongWritable single_count = new LongWritable(Long.parseLong(split[2]));
                Random rand = new Random();
                int n = rand.nextInt(2);
                if (n % 2 == 0) {
                    map.put(new LongWritable(0), single_count);
                } else {
                    map.put(new LongWritable(0), single_count);
                }
                context.write(threegram , map);
            }
            @Override
            public void cleanup(Context context)  throws IOException, InterruptedException {
            }
        }


        public static class CombinerClass extends Reducer<Text,helperMap,Text,helperMap> {
            @Override
            public void reduce(Text line, Iterable<helperMap> maps, Context context) throws IOException,  InterruptedException {
                // for a given 3gram, we sum all his occourances in both partitions, we create a map for each partition and send it with the 3gram
                long p0_count = 0;
                long p1_count = 0;
                for (helperMap map : maps) {
                    LongWritable partition = (LongWritable)map.keySet().iterator().next();
                    LongWritable single_count = (LongWritable)map.values().iterator().next();
                    if (partition.get() == 0 ){
                       p0_count += single_count.get();
                    } else {
                        p1_count += single_count.get();
                    }
                }
                helperMap p0_map = new helperMap();
                helperMap p1_map = new helperMap();
                p0_map.put(new LongWritable(0) , new LongWritable(p0_count));
                p1_map.put(new LongWritable(1), new LongWritable(p1_count));

                context.write(line, p0_map);
                context.write(line, p1_map);
            }

            @Override
            public void cleanup(Context context)  throws IOException, InterruptedException {
            }
        }

        public static class ReducerClass extends Reducer<Text, helperMap, Text, helperMap> {
            private Counter n;

            @Override
            public void setup(Context context)  throws IOException, InterruptedException {
                n = context.getCounter("N", "N");
            }

            @Override
            public void reduce(Text line, Iterable<helperMap> maps, Context context) throws IOException,  InterruptedException {
                // for a given 3gram, we sum all his occourances in both partitions, we create a map for each partition and send it with the 3gram
                long p0_count = 0, p1_count = 0;
                for (helperMap map : maps) {
                    LongWritable partition = (LongWritable)map.keySet().iterator().next();
                    LongWritable single_count = (LongWritable)map.values().iterator().next();
                    if (partition.get() == 0 ){
                       p0_count += single_count.get();
                    } else {
                        p1_count += single_count.get();
                    }
                }
                helperMap bothCounts = new helperMap();
                bothCounts.put(new LongWritable(p0_count), new LongWritable(p1_count)); 
                context.write(line, bothCounts); // write the 3gram and the map of both counts to the context
                n.increment(p0_count + p1_count); // increment the total N
            }
        }

        public static class PartitionerClass extends Partitioner<Text, helperMap> {
            @Override
            public int getPartition(Text key, helperMap value, int numPartitions) {
                return (key.hashCode() & Integer.MAX_VALUE ) % numPartitions;
            }
        }
}
