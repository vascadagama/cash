package com.tuplejump.hive.metastore.model;

import java.util.List;

/**
 * Created by rohit on 2/1/14.
 */
public class CColumnDescriptor {

    private List<CFieldSchema> cols;

    public CColumnDescriptor() {
    }

    public CColumnDescriptor(List<CFieldSchema> cols) {
        this.cols = cols;
    }

    public List<CFieldSchema> getCols() {
        return cols;
    }

    public void setCols(List<CFieldSchema> cols) {
        this.cols = cols;
    }
}
