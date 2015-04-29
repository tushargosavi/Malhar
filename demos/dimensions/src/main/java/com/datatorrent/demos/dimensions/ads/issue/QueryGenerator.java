package com.datatorrent.demos.dimensions.ads.issue;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import java.util.Timer;
import java.util.TimerTask;

public class QueryGenerator extends BaseOperator implements InputOperator {

    private boolean emitted = false;
    DefaultOutputPort<String> out = new DefaultOutputPort<String>();
    private String queryStr = "{ \"id\": 1024, \"keys\": { \"publisherId\": \"4\" }}";

    private transient Timer timer;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                emitted = false;
            }
        }, 1000, 1000);
    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    @Override
    public void emitTuples() {
        if (!emitted) {
            out.emit(queryStr);
            emitted = true;
        }
    }
}
