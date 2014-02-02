package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;

/**
 * Created by rohit on 2/1/14.
 */

@Entity
public class CFieldSchema implements Serializable {
    @Id
    @Column
    private String name;

    @Column
    private String type;

    @Column
    private String comment;

    public CFieldSchema() {
    }

    public CFieldSchema(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }

    public CFieldSchema(FieldSchema fs) {
        this(fs.getName(), fs.getType(), fs.getComment());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public FieldSchema toFieldSchema() {
        return new FieldSchema(this.getName(), this.getType(), this.getComment());
    }
}
