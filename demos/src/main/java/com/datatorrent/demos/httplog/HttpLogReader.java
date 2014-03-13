package com.datatorrent.demos.httplog;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by tugosavi on 3/10/14.
 */
public class HttpLogReader extends SimpleSinglePortInputOperator<String> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HttpLogReader.class);
    protected long averageSleep = 300;
    protected long sleepPlusMinus = 100;
    protected String fileName = "/home/tugosavi/Downloads/BigData/mapreduce/access_log";
    private transient BufferedReader br;
    private transient DataInputStream in;


    public void setAverageSleep(long as) {
        averageSleep = as;
    }

    public void setSleepPlusMinus(long spm) {
        sleepPlusMinus = spm;
    }

    public void setFileName(String fn) {
        fileName = fn;
    }

    public void run() {
        while (true) {
            try {
                String line;
                FileInputStream fstream = new FileInputStream(fileName);
                in = new DataInputStream(fstream);
                br = new BufferedReader(new InputStreamReader(in));

                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    outputPort.emit(line);
                }
                try {
                    Thread.sleep(averageSleep + (new Double(sleepPlusMinus * (Math.random() * 2 - 1))).longValue());
                } catch (InterruptedException ex) {

                }
                br.close();
                in.close();
                fstream.close();
            } catch(IOException ex) {
                System.out.println("Exception occured " + ex.toString());
                logger.debug(ex.toString());
            }
        }
    }
}
