package com.datatorrent.demos.httplog;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by tugosavi on 3/10/14.
 */
public class LogUrlExtractor extends BaseOperator {
    public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String s) {
            String[] resource = s.trim().split(" ");
            url.emit(resource[6]);
        }
    };
}
