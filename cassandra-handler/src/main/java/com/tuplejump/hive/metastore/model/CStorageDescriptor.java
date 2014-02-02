package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import javax.persistence.Column;
import javax.persistence.Entity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by rohit on 2/1/14.
 */
@Entity
public class CStorageDescriptor {
    @Column
    private List<CFieldSchema> cd;

    @Column
    private String location;

    @Column
    private String inputFormat;

    @Column
    private String outputFormat;

    @Column
    private boolean isCompressed = false;

    @Column
    private int numBuckets = 1;

    @Column
    private CSerDeInfo serDeInfo;

    @Column
    private List<String> bucketCols;

    @Column
    private List<COrder> sortCols;

    @Column
    private Map<String, String> parameters;

    public CStorageDescriptor() {

    }

    public CStorageDescriptor(List<CFieldSchema> cd, String location, String inputFormat,
                              String outputFormat, boolean isCompressed, int numBuckets, CSerDeInfo serDeInfo,
                              List<String> bucketCols, List<COrder> sortCols, Map<String, String> parameters) {
        this.cd = cd;
        this.location = location;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.isCompressed = isCompressed;
        this.numBuckets = numBuckets;
        this.serDeInfo = serDeInfo;
        this.bucketCols = bucketCols;
        this.sortCols = sortCols;
        this.parameters = parameters;
    }

    public CStorageDescriptor(StorageDescriptor sd) {
        this(new ArrayList<CFieldSchema>(), sd.getLocation(), sd.getInputFormat(),
                sd.getOutputFormat(), sd.isCompressed(), sd.getNumBuckets(),
                new CSerDeInfo(sd.getSerdeInfo()), sd.getBucketCols(), new ArrayList<COrder>(), sd.getParameters());

        for (FieldSchema fs : sd.getCols()) {
            this.cd.add(new CFieldSchema(fs));
        }

        for (Order o : sd.getSortCols()) {
            this.sortCols.add(new COrder(o));
        }
    }

    public List<CFieldSchema> getCd() {
        return cd;
    }

    public void setCd(List<CFieldSchema> cd) {
        this.cd = cd;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public CSerDeInfo getSerDeInfo() {
        return serDeInfo;
    }

    public void setSerDeInfo(CSerDeInfo serDeInfo) {
        this.serDeInfo = serDeInfo;
    }

    public List<String> getBucketCols() {
        return bucketCols;
    }

    public void setBucketCols(List<String> bucketCols) {
        this.bucketCols = bucketCols;
    }

    public List<COrder> getSortCols() {
        return sortCols;
    }

    public void setSortCols(List<COrder> sortCols) {
        this.sortCols = sortCols;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    StorageDescriptor toStorageDescriptor() {
        List<FieldSchema> fs = new ArrayList<FieldSchema>();
        for (CFieldSchema cfs : cd) {
            fs.add(cfs.toFieldSchema());
        }

        List<Order> orders = new ArrayList<Order>();

        for (COrder col : sortCols) {
            orders.add(col.toOrder());
        }

        return new StorageDescriptor(fs, this.location, this.inputFormat, this.outputFormat, this.isCompressed,
                this.numBuckets, this.serDeInfo.toSerDeInfo(), this.bucketCols, orders, this.parameters);
    }
}
