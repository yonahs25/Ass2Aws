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
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import static mle.mleManager.totalN;
import java.util.Random;
import java.util.Set;


// First Job - partitions the 3grams into 2 partitions and calculates the N for each partition (N0 and N1)
// Calculate total N
public class partitionsAndN {

        public static class MapperClass extends Mapper<LongWritable, Text, Text, helperMap> {

            // All the English stop words.
            private String[] stopWordsArr = {"a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", 
            "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};
            private Set<String> stopWords = new HashSet<String>(Arrays.asList(stopWordsArr)); //keeping as Set for faster lookup

            @Override
            public void setup(Context context)  throws IOException, InterruptedException {

            }

            @Override
            public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
                Text threegram = new Text();
                helperMap map = new helperMap();
                String[] split_line = line.toString().split(",");
                for (String word : split_line[0].split(" ")){
                    if (stopWords.contains(word)) return; //checking for stop words in the trigram
                }
                //TODO need to check if 3gram  is valid (from the banned words)
                LongWritable single_count = new LongWritable(Long.parseLong(split_line[2]));
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
