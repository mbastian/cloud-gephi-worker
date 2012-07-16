package org.gephi.cloud.worker;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author mbastian
 */
public class AmazonClient {

    //Logger
    private static final Logger logger = Logger.getLogger(AmazonClient.class.getName());
    //Config static
    private static final int NUM_MESSAGES = 1;
    private static final int NUM_THREADS = 1;
    //S3
    private final String inputBucketName;
    private final String outputBucketName;
    private final TransferManager transferManager;
    private final AmazonS3 s3Client;
    private final List<Upload> uploads;
    //SQS
    private final String inputQueueUrl;
    private final AmazonSQSClient sqsClient;
    private final String outputQueueUrl;

    public AmazonClient(Properties properties) {
        //Pull properties values
        String accessKey = properties.getProperty("access_key");
        String secretKey = properties.getProperty("secret_key");
        inputBucketName = properties.getProperty("input_bucket_name");
        outputBucketName = properties.getProperty("output_bucket_name");
        inputQueueUrl = properties.getProperty("input_queue_url");
        outputQueueUrl = properties.getProperty("output_queue_url");

        //Init credentials
        logger.log(Level.INFO, "Credentials:\n\t{0}\n\t{1}", new Object[]{accessKey, secretKey});
        AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);

        //Init clients
        s3Client = new AmazonS3Client(myCredentials);
        sqsClient = new AmazonSQSClient(myCredentials);

        //Create input bucket if not exist
        if (!s3Client.doesBucketExist(inputBucketName)) {
            s3Client.createBucket(inputBucketName);
            logger.log(Level.INFO, "Create input bucket {0}", inputBucketName);
        }

        //Create output bucket if not exist
        if (!s3Client.doesBucketExist(outputBucketName)) {
            s3Client.createBucket(outputBucketName);
            logger.log(Level.INFO, "Create output bucket {0}", outputBucketName);
        }

        //Init S3 manager
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_THREADS, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "S3 Upload");
                return t;
            }
        });
        logger.log(Level.INFO, "Started Transfer Manager on {0} threads", NUM_THREADS);
        transferManager = new TransferManager(s3Client, executor);
        uploads = Collections.synchronizedList(new ArrayList<Upload>());
    }

    /**
     * Pull messages from the input queue. By default, only one message is
     * pulled.
     *
     * @return a list of messages received
     */
    public List<Message> getMessages(String queue) {
        ReceiveMessageRequest rmr = new ReceiveMessageRequest(queue);
        rmr.setMaxNumberOfMessages(NUM_MESSAGES);
        rmr.setVisibilityTimeout(10000);
        ReceiveMessageResult result = sqsClient.receiveMessage(rmr);
        List<Message> msgs = result.getMessages();
        logger.log(Level.INFO, "Pulled {0} messages from the {1} queue", new Object[]{msgs.size(), queue});
        for (Message m : msgs) {
            DeleteMessageRequest dmr = new DeleteMessageRequest(queue, m.getReceiptHandle());
            sqsClient.deleteMessage(dmr);
        }
        return msgs;
    }

    /**
     * Push message to the output queue
     *
     * @param body the message body
     */
    public void sendMessages(String body, String queue) {
        SendMessageRequest smr = new SendMessageRequest();
        smr.setQueueUrl(queue);
        smr.setMessageBody(body);
        sqsClient.sendMessage(smr);
    }

    public void upload(byte[] bytes, String bucket, String contentType, String key, String fileName) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(bytes.length);
        objectMetadata.setContentType(contentType);
        objectMetadata.setContentDisposition(fileName);

        Upload upload = transferManager.upload(bucket, key, bis, objectMetadata);
        uploads.add(upload);
        logger.log(Level.INFO, "Start upload of {0} bytes to key={1} on bucket={2}", new Object[]{bytes.length, key, bucket});
    }

    public byte[] download(String key, String bucket) {
        String bucketName = bucket;
        InputStream stream = s3Client.getObject(bucketName, key).getObjectContent();
        try {
            BufferedInputStream bis = new BufferedInputStream(stream);
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            int c = bis.read();
            while (c != -1) {
                out.write(c);
                c = bis.read();
            }
            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public BufferedReader downloadCompressedStream(String key) {
        String bucketName = inputBucketName;
        InputStream stream = s3Client.getObject(bucketName, key).getObjectContent();
        try {
            GZIPInputStream bis = new GZIPInputStream(stream);
            InputStreamReader reader = new InputStreamReader(bis);
            BufferedReader in = new BufferedReader(reader);
            return in;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void cleanFolder(String key) {
        LinkedList<String> stack = new LinkedList<String>();
        stack.add(key);
        for (; !stack.isEmpty();) {
            String k = stack.pollFirst();
            ObjectListing list = s3Client.listObjects(outputBucketName, k);
            stack.addAll(list.getCommonPrefixes());
            for (S3ObjectSummary o : list.getObjectSummaries()) {
                s3Client.deleteObject(outputBucketName, o.getKey());
            }
        }
    }

    public void cleanFile(String key, String bucket) {
        s3Client.deleteObject(bucket, key);
    }

    public void finishUploads() {
        for (Upload u : uploads.toArray(new Upload[0])) {
            try {
                u.waitForCompletion();
            } catch (Exception e) {
                logger.throwing(getClass().getName(), "finishUploads", e);
            }
            logger.log(Level.INFO, "{0} finished upload", u.getDescription());
        }
    }

    public void shutdownNow() {
        transferManager.shutdownNow();
    }

    public String getInputBucketName() {
        return inputBucketName;
    }

    public String getOutputBucketName() {
        return outputBucketName;
    }

    public String getInputQueueUrl() {
        return inputQueueUrl;
    }

    public String getOutputQueueUrl() {
        return outputQueueUrl;
    }
}
