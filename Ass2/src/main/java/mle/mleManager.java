package mle;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;

import mle.jobs.calcNr4Partition;
import mle.jobs.calcTr4Partition;
import mle.jobs.partitionsAndN;
import mle.jobs.sumXr4Partition;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.time.LocalTime;  
import java.io.IOException;




public class mleManager {
    public static enum totalN{ N }
    private static String input_bucket;
    private static String output_bucket;

    private static String set_input_output_path_for_job (Job job , String input_path)  throws IOException{
        FileInputFormat.addInputPath(job, new Path(input_path));
        String path = output_bucket + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static String partitionAndN_setter (Job job, String path) throws IOException {
        job.setJarByClass(partitionsAndN.class);
        job.setMapperClass(partitionsAndN.MapperClass.class);
        job.setReducerClass(partitionsAndN.ReducerClass.class);
        job.setCombinerClass(partitionsAndN.CombinerClass.class);
        job.setPartitionerClass(partitionsAndN.PartitionerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(helperMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(helperMap.class);

        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return set_input_output_path_for_job(job, path);
    }

    private static String calc_Nr_for_partition_setter(Job job , String path) throws IOException {
        job.setJarByClass(calcNr4Partition.class);
        job.setMapperClass(calcNr4Partition.MapperClass.class);
        job.setPartitionerClass(calcNr4Partition.PartitionerClass.class);
        job.setCombinerClass(calcNr4Partition.ReducerClass.class);
        job.setReducerClass(calcNr4Partition.ReducerClass.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return set_input_output_path_for_job(job, path);
    }

    private static String calc_Tr_for_partition_setter(Job job , String Path ) throws IOException {
        job.setJarByClass(calcTr4Partition.class);
        job.setMapperClass(calcTr4Partition.MapperClass.class);
        job.setPartitionerClass(calcTr4Partition.PartitionerClass.class);
        job.setCombinerClass(calcTr4Partition.ReducerClass.class);
        job.setReducerClass(calcTr4Partition.ReducerClass.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);



        return set_input_output_path_for_job(job, Path);
    }

    private static String sum_Xr_for_partition_setter(Job job, String splited_file_path, String Xr_file_path) 
                                                                                    throws IOException {
        job.setJarByClass(sumXr4Partition.class);
        job.setPartitionerClass(sumXr4Partition.PartitionerClass.class);
        job.setCombinerClass(sumXr4Partition.ReducerClass.class);
        job.setReducerClass(sumXr4Partition.ReducerClass.class);

        MultipleInputs.addInputPath(job, new Path(splited_file_path),
        TextInputFormat.class, sumXr4Partition.MapperClassPartition.class);
        MultipleInputs.addInputPath(job, new Path (Xr_file_path),
                TextInputFormat.class, sumXr4Partition.MapperClass4Xr.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
                
        String path = output_bucket + job.getJobName();
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    

    public static void main (String[] args)  throws IOException ,   ClassNotFoundException, InterruptedException {
        if (args.length < 2){
            System.out.println("need to provide input and output please");
            System.exit(1);
        }

        input_bucket = args[0];
        output_bucket = args[1];

        Configuration partitionsAndN_config = new Configuration();
        final Job partitionsAndN_job = Job.getInstance(partitionsAndN_config, "Split");
        String partitionsAndN_path = partitionAndN_setter(partitionsAndN_job , input_bucket);
        if (partitionsAndN_job.waitForCompletion(true)) {
            System.out.println("finished counter and splitting successfully");
        } else {
            System.out.println("failed counter and splitting");
            System.exit(1);
        }


        Configuration calc_Nr0_config = new Configuration();
        calc_Nr0_config.setBoolean("partition", true);
        final Job calc_Nr0_job = Job.getInstance(calc_Nr0_config, "calc_Nr0");
        String calc_Nr0_path = calc_Nr_for_partition_setter(calc_Nr0_job, partitionsAndN_path);
        if (calc_Nr0_job.waitForCompletion(true)) {
            System.out.println("finished calculation all Nr for partition 0 successfully");
        } else {
            System.out.println("failed calculation all Nr for partition 0");
            System.exit(1);
        }

        Configuration calc_Nr1_config = new Configuration();
        calc_Nr1_config.setBoolean("partition", false);
        final Job calc_Nr1_job = Job.getInstance(calc_Nr1_config, "calc_Nr1");
        String calc_Nr1_path = calc_Nr_for_partition_setter(calc_Nr1_job, partitionsAndN_path);
        if (calc_Nr1_job.waitForCompletion(true)) {
            System.out.println("finished calculation all Nr for partition 1 successfully");
        } else {
            System.out.println("failed calculation all Nr for partition 0");
            System.exit(1);
        }

        Configuration calc_Tr0_config = new Configuration();
        calc_Tr0_config.setStrings("direction", "01");
        final Job calc_Tr0_job = Job.getInstance(calc_Tr0_config, "calc_Tr0");
        String calc_Tr0_path = calc_Tr_for_partition_setter(calc_Tr0_job, calc_Nr0_path);
        if (calc_Tr0_job.waitForCompletion(true)) {
            System.out.println("finished calculation all Tr for partition 0 successfully");
        } else {
            System.out.println("failed calculation all Tr for partition 0");
            System.exit(1);
        }

        Configuration calc_Tr1_config = new Configuration();
        calc_Tr1_config.setStrings("direction", "10");
        final Job calc_Tr1_job = Job.getInstance(calc_Tr1_config, "calc_Tr1");
        String calc_Tr1_path = calc_Tr_for_partition_setter(calc_Tr1_job, calc_Nr1_path);
        if (calc_Tr1_job.waitForCompletion(true)) {
            System.out.println("finished calculation all Tr for partition 1 successfully");
        } else {
            System.out.println("failed calculation all Tr for partition 1");
            System.exit(1);
        }    
    }
    
}
