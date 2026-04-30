package com.metrics.flink.model;

import java.io.Serial;
import java.io.Serializable;

public class ExternalDataRecord implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String sourceApi;
    private String jsonPayload;
    private long receivedAt;

    public ExternalDataRecord() {}

    public ExternalDataRecord(String sourceApi, String jsonPayload, long receivedAt) {
        this.sourceApi   = sourceApi;
        this.jsonPayload = jsonPayload;
        this.receivedAt  = receivedAt;
    }

    public String getSourceApi()    { return sourceApi; }
    public String getJsonPayload()  { return jsonPayload; }
    public long   getReceivedAt()   { return receivedAt; }

    public void setSourceApi(String sourceApi)      { this.sourceApi = sourceApi; }
    public void setJsonPayload(String jsonPayload)  { this.jsonPayload = jsonPayload; }
    public void setReceivedAt(long receivedAt)      { this.receivedAt = receivedAt; }
}
