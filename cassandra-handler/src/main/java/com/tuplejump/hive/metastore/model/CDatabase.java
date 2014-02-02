package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by rohit on 1/30/14.
 */
@Entity
public class CDatabase {
    @Id
    @Column
    private String name;

    @Column
    private String locationUri;

    @Column
    private String description;

    @Column
    private Map<String, String> parameters;

    @Column
    private List<String> tables;


    public CDatabase() {
        this.tables = new ArrayList<String>();
    }

    public CDatabase(String name, String description, String locationUri, Map<String, String> parameters) {
        this.name = name;
        this.locationUri = locationUri;
        this.description = description;
        this.parameters = parameters;
        this.tables = new ArrayList<String>();
    }

    public CDatabase(String name, String description, String locationUri) {
        this.name = name;
        this.locationUri = locationUri;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocationUri() {
        return locationUri;
    }

    public void setLocationUri(String locationUri) {
        this.locationUri = locationUri;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(String name, String value) {
        parameters.put(name, value);
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public void addTable(String t) {
        this.tables.add(t);
    }
}
