package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Type;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rohit on 2/1/14.
 */

@Entity
public class CType {

    @Id
    @Column
    private String name;

    @Column
    private String type1;

    @Column
    private String type2;

    @Column
    private List<CFieldSchema> fields;

    public CType() {
    }

    public CType(String name, String type1, String type2) {
        this.name = name;
        this.type1 = type1;
        this.type2 = type2;
    }

    public CType(String name, String type1, String type2, List<CFieldSchema> fields) {
        this.name = name;
        this.type1 = type1;
        this.type2 = type2;
        this.fields = fields;
    }

    public CType(Type t) {
        this(t.getName(), t.getType1(), t.getType2());

        fields = new ArrayList<CFieldSchema>();
        for (FieldSchema fs : t.getFields()) {
            fields.add(new CFieldSchema(fs));
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType1() {
        return type1;
    }

    public void setType1(String type1) {
        this.type1 = type1;
    }

    public String getType2() {
        return type2;
    }

    public void setType2(String type2) {
        this.type2 = type2;
    }

    public List<CFieldSchema> getFields() {
        return fields;
    }

    public void setFields(List<CFieldSchema> fields) {
        this.fields = fields;
    }

    public Type toType() {
        List<FieldSchema> tfields = new ArrayList<FieldSchema>();

        for (CFieldSchema cfs : this.getFields()) {
            tfields.add(cfs.toFieldSchema());
        }

        Type t = new Type(this.getName());
        t.setType1(this.getType1());
        t.setType2(this.getType2());
        t.setFields(tfields);

        return t;
    }
}
