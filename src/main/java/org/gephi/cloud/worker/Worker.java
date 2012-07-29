package org.gephi.cloud.worker;

import com.amazonaws.services.sqs.model.Message;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;

public class Worker {

    //Logger
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    //Conf
    private static final int DELAY = 1000;
    //Stuff
    private final Properties properties;
    private final AmazonClient awsClient;
    private final ObjectMapper mapper = new ObjectMapper();
    //State
    private boolean stop = false;
    //

    public Worker() {
        //Load properties
        properties = loadProperties();

        //Init Amazon Client
        awsClient = new AmazonClient(properties);
    }

    /**
     * Continuously run the worker and pull messages every second
     */
    public void runContinuously() {
        while (!stop) {
            run();
        }

        //Cleanup
        awsClient.finishUploads();
        awsClient.shutdownNow();
    }
    
    public void run() {
        //Coninuously pull and process messages
        List<Message> messages = awsClient.getMessages(awsClient.getInputQueueUrl());
        while (messages != null && !messages.isEmpty()) {
            logger.log(Level.INFO, "Worker received {0} messages to process. Starting... ", messages.size());
            for (Message message : messages) {
                String msg = message.getBody();
                logger.log(Level.INFO, "Starting processing message={0}", msg.substring(0, Math.min(msg.length(), 100)));
                processMessage(message);
                logger.log(Level.INFO, "End processing");
            }
            awsClient.deleteMessages(messages, awsClient.getInputQueueUrl());
            messages = awsClient.getMessages(awsClient.getInputQueueUrl());
        }
        try {
            logger.log(Level.INFO, "Now sleeping {0} ms", DELAY);
            Thread.sleep(DELAY);
        } catch (InterruptedException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        logger.log(Level.INFO, "Stopping worker...");
    }
    
    public void stop() {
        stop = true;
    }
    
    private void processMessage(Message message) {
        try {
            //Unserialize job message
            JobMessage job = unserializeJob(message.getBody());
            String fileKey = job.getFileKey();
            String projectName = fileKey.substring(0, fileKey.indexOf("/"));

            //Write dummy output on S3
            awsClient.upload("foo".getBytes(), awsClient.getOutputBucketName(), "text/plain", projectName + "/result.txt", "result.txt");
            awsClient.finishUploads();

            //Send message to the ouptut queue
            String callback = message.getBody();
            awsClient.sendMessages(callback, awsClient.getOutputQueueUrl());
        } catch (IOException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String serializeJob(JobMessage job) throws IOException {
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, job);
        return writer.toString();
    }
    
    public JobMessage unserializeJob(String jobString) throws IOException {
        return mapper.readValue(jobString, JobMessage.class);
    }
    
    private Properties loadProperties() {
        Properties prop = new Properties();
        
        try {
            //load a properties file
            prop.load(getClass().getResourceAsStream("/aws.properties"));
            return prop;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
    
    public Properties getProperties() {
        return properties;
    }
    
    public static void main(String[] args) {
        Worker app = new Worker();
        app.run();
    }
}
