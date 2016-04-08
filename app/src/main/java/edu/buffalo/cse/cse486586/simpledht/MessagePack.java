package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import android.content.ContentValues;

import org.json.JSONArray;
import org.json.JSONObject;

public class MessagePack implements Serializable {
    protected String nodeId;
    protected String key;
    protected String value;
    protected String msgType;

    MessagePack(String nodeId, String key, String value, String msgType){
        this.nodeId = nodeId;
        this.key = key;
        this.value = value;
        this.msgType = msgType;
    }

}
