package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.Order;

import javax.jdo.annotations.Column;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Created by rohit on 2/1/14.
 */
@Entity
public class COrder implements Serializable {
    @Column
    private String col;
    @Column
    private int order;

    public COrder() {
    }

    public COrder(Order o) {
        this(o.getCol(), o.getOrder());
    }

    public COrder(String col, int order) {
        this.col = col;
        this.order = order;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    Order toOrder(){
        return new Order(this.col, this.order);
    }
}
