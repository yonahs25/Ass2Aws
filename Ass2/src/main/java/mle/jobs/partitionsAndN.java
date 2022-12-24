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
import static mle.mleManager.totalN;
import java.util.Random;

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
         /*
         *   3GRAM = [ {0 , 1} , {1,1} .....]
         *  =>
         * 3GRAM [ {1 , X } , {0, Y}]
         * 
         */
            @Override
            public void reduce(Text line, Iterable<helperMap> maps, Context context) throws IOException,  InterruptedException {
                long p0 = 0;
                long p1 = 0;
                for (helperMap map : maps) {
                    LongWritable partition = (LongWritable)map.keySet().iterator().next();
                    LongWritable single_count = (LongWritable)map.values().iterator().next();
                    if (partition.get() == 0 ){
                       p0 += single_count.get();
                    } else {
                        p1 += single_count.get();
                    }
                }

            }





        }




    

    
    
}
