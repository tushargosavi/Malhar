package com.datatorrent.demos.uniquevaluetest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tushar on 7/4/14.
 */
public class RandomDataGenerator implements InputOperator {
    public final transient DefaultOutputPort<KeyValPair<String, Object>> outport = new DefaultOutputPort<KeyValPair<String, Object>>();
    private HashMap<String, Integer> dataInfo;

    public RandomDataGenerator() {
        this.dataInfo = new HashMap<String, Integer>();
        this.dataInfo.put("a", 1000);
        this.dataInfo.put("b", 1000);
        this.dataInfo.put("c", 1000);
        this.dataInfo.put("d", 1000);
    }

    @Override
    public void emitTuples() {
        for(Map.Entry<String, Integer> e : dataInfo.entrySet()) {
            String key = e.getKey();
            int count = e.getValue();
            for(int i = 0; i < count; ++i) {
                outport.emit(new KeyValPair<String, Object>(key, i));
            }
        }
    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {

    }

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }
}
