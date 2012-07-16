package org.gephi.cloud.worker;

/**
 *
 * @author mbastian
 */
public class JobMessage {

    public enum MessageType {

        RENDER
    };
    private MessageType type;
    private String fileKey;
    private String[] params;

    public JobMessage() {
    }

    public JobMessage(MessageType type, String fileKey, String[] params) {
        this.type = type;
        this.fileKey = fileKey;
        this.params = params;
    }

    public MessageType getType() {
        return type;
    }

    public String getFileKey() {
        return fileKey;
    }

    public String[] getParams() {
        return params;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public void setParams(String[] params) {
        this.params = params;
    }
}
