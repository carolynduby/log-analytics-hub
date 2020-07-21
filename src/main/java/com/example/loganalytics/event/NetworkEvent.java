package com.example.loganalytics.event;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class NetworkEvent extends LogEvent {
    public static final String IP_DST_ADDR_FIELD = "ip_dst_addr";
    public static final String IP_SRC_ADDR_FIELD = "ip_src_addr";
    public static final String IP_DST_PORT_FIELD = "ip_dst_port";
    public static final String IP_SRC_PORT_FIELD = "ip_src_port";
    public static final String PROTO_FIELD = "proto";
    public static final String TRANS_ID_FIELD = "trans_id";

    NetworkEvent(Map<String, Object> fields) {
        super(fields);
    }
    @JsonIgnore
    public String getSourceIp() {
        return (String) getField(IP_SRC_ADDR_FIELD);
    }

    @JsonIgnore
    public long getSourcePort() {
        return  (long)getField(IP_SRC_PORT_FIELD);
    }

    @JsonIgnore
    public String getDestinationIp() {
        return (String) getField(IP_DST_ADDR_FIELD);
    }

    @JsonIgnore
    public long getDestinationPort() {
        return (long) getField(IP_DST_PORT_FIELD);
    }
}
