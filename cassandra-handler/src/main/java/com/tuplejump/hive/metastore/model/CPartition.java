package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;

import javax.jdo.annotations.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.List;
import java.util.Map;

/**
 * Created by rohit on 2/1/14.
 */
@Entity
public class CPartition {
    @Id
    @Column
    private String partitionName; // partitionname ==>  (key=value/)*(key=value)

    @Column
    private String dbName;

    @Column
    private String table;

    @Column
    private List<String> values;

    @Column
    private int createTime;

    @Column
    private int lastAccessTime;

    @Column
    private CStorageDescriptor sd;

    @Column
    private Map<String, String> parameters;

    @Column
    private List<CPartitionEvent> events;

    public CPartition() {

    }

    public CPartition(String partitionName, String dbName, String table, List<String> values, int createTime, int lastAccessTime, CStorageDescriptor sd, Map<String, String> parameters) {
        this.partitionName = partitionName;
        this.table = table;
        this.dbName = dbName;
        this.values = values;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.sd = sd;
        this.parameters = parameters;
    }

    public CPartition(String partName, Partition p) {
        this(partName, p.getDbName(), p.getTableName(), p.getValues(), p.getCreateTime(), p.getLastAccessTime(), new CStorageDescriptor(p.getSd()), p.getParameters());
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public CStorageDescriptor getSd() {
        return sd;
    }

    public void setSd(CStorageDescriptor sd) {
        this.sd = sd;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public List<CPartitionEvent> getEvents() {
        return events;
    }

    public void setEvents(List<CPartitionEvent> events) {
        this.events = events;
    }

    public void addEvent(CPartitionEvent cpe) {
        this.events.add(cpe);
    }

    public void removeEvent(CPartition cpe) {
        this.events.remove(cpe);
    }

    public Partition toPartition() {
        return new Partition(this.values, this.dbName, this.table, this.createTime, this.lastAccessTime,
                this.sd.toStorageDescriptor(), this.parameters);
    }

    @Entity
    public static class CPartitionEvent {

        @Column
        private long eventTime;

        @Column
        private int eventType;

        public CPartitionEvent() {
        }

        public CPartitionEvent(long eventTime, int eventType) {
            this.eventTime = eventTime;
            this.eventType = eventType;
        }

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public int getEventType() {
            return eventType;
        }

        public void setEventType(int eventType) {
            this.eventType = eventType;
        }
    }
}
