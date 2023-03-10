package xmleRunner;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
public class App 
{
    private static String bucketName, keyName, serviceRole, jobFlowRole;
    public static void main( String[] args ) throws IOException
    {
        AWSCredentialsProvider cp = new ProfileCredentialsProvider();
        AWSCredentials credentials = cp.getCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        bucketName = "ohad-and-yonah-done-bucket";
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://oy-jars-bucket/Ass2-1.0-SNAPSHOT.jar")
                .withMainClass("mle.mleManager")
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-fiction-all/3gram/data",
                        "s3n://" + bucketName + "/output");

        StepConfig stepConfig = new StepConfig()
                .withName("Calculate_Deleted_Estimations")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(9)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("something")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        serviceRole = "EMR_DefaultRole";
        jobFlowRole = "EMR_EC2_DefaultRole";

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Deleted Estimations on English 3Gram")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole(serviceRole)
                .withJobFlowRole(jobFlowRole)
                .withLogUri("s3n://" + bucketName + "/logs/")
                .withReleaseLabel("emr-4.2.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
    }
}
