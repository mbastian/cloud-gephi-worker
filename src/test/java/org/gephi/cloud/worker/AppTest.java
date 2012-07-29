package org.gephi.cloud.worker;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        try {
            Properties properties = new Properties();
            properties.setProperty("input_bucket_name", "cloudgephitestinput");
            properties.setProperty("output_bucket_name", "cloudgephitestoutput");
            properties.setProperty("input_queue_url", "https://queue.amazonaws.com/992095333379/cloudgephijobstest");
            properties.setProperty("output_queue_url", "https://queue.amazonaws.com/992095333379/cloudgephicallbackstest");
            Worker worker = new Worker(properties);

            AmazonClient awsClient = new AmazonClient(worker.getProperties());

            //Clean files
            awsClient.cleanFile("foo/sample.gexf", awsClient.getInputBucketName());
            awsClient.cleanFile("foo/result.txt", awsClient.getOutputBucketName());

            //Load sample GEXF
            InputStream gexfStrem = getClass().getResourceAsStream("/sample.gexf");
            byte[] gexfData = ByteStreams.toByteArray(gexfStrem);
            gexfStrem.close();

            //Upload it to the jobs under the foo project
            awsClient.upload(gexfData, awsClient.getInputBucketName(), "application/gexf+xml", "foo/sample.gexf", "sample.gexf");
            awsClient.finishUploads();

            //Send message on the inputqueue
            JobMessage job = new JobMessage(JobMessage.MessageType.RENDER, "foo/sample.gexf", null);
            String serializedMessage = worker.serializeJob(job);
            Logger.getLogger(AppTest.class.getName()).log(Level.INFO, "Sending message: {0}", serializedMessage);
            awsClient.sendMessages(serializedMessage, awsClient.getInputQueueUrl());

            //Wait a little bit so the message is in the input queue
            Thread.sleep(2000);

            //Run the worker - just once
            worker.run();

            //Look for result file on S3
            String resultStr = new String(awsClient.download("foo/result.txt", awsClient.getOutputBucketName()));
            assertEquals("foo", resultStr);

            //Wait a little bit so the message is in the output queue
            Thread.sleep(2000);

            //Look if received message on output queue
            List<Message> msgs = awsClient.getMessages(awsClient.getOutputQueueUrl());
            awsClient.deleteMessages(msgs, awsClient.getOutputQueueUrl());
            assertEquals(1, msgs.size());
            Message msg = msgs.get(0);
            assertEquals(serializedMessage, msg.getBody());

        } catch (InterruptedException ex) {
            Logger.getLogger(AppTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AppTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
