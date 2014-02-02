package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import javax.persistence.Column;
import javax.persistence.Entity;
import java.util.Map;

/**
 * Created by rohit on 2/1/14.
 */

@Entity
public class CSerDeInfo {

    public CSerDeInfo() {
    }

    public CSerDeInfo(String name, String serializationLib, Map<String, String> parameters) {
        this.name = name;
        this.serializationLib = serializationLib;
        this.parameters = parameters;
    }


    public CSerDeInfo(SerDeInfo sdi) {
        this(sdi.getName(), sdi.getSerializationLib(), sdi.getParameters());
    }

    @Column
    private String name;

    @Column
    private String serializationLib;

    @Column
    private Map<String, String> parameters;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    public void setSerializationLib(String serializationLib) {
        this.serializationLib = serializationLib;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    SerDeInfo toSerDeInfo() {
        return new SerDeInfo(this.name, this.serializationLib, this.parameters);
    }
}