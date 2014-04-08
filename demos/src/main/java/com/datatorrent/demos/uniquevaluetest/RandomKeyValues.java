/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.uniquevaluetest;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;

import java.util.Random;

/**
 * Input port operator for generating random values on keys. <br>
 * Key(s)   : key + integer in range between 0 and numKeys <br>
 * Value(s) : integer in range of 0 to numValuesPerKeys <br>
 * @since 0.9.3
 */
public class RandomKeyValues implements InputOperator
{

	public final transient DefaultOutputPort<KeyValPair<String, Object>> outport = new DefaultOutputPort<KeyValPair<String, Object>>();
	private Random random = new Random(11111);
    private int numKeys;
    private int numValuesPerKeys;
    private int tuppleBlast = 1000;
    private int emitDelay = 20; /* 20 ms */

    public RandomKeyValues() {
        this.numKeys = 100;
        this.numValuesPerKeys = 100;
    }

    public RandomKeyValues(int keys, int values) {
        this.numKeys = keys;
        this.numValuesPerKeys = values;
    }

    @Override
	public void beginWindow(long windowId)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void endWindow()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setup(OperatorContext context)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void teardown()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void emitTuples()
	{
        /* generate tuples randomly, */
        for(int i = 0; i < tuppleBlast; i++) {
            String key = "key" + String.valueOf(random.nextInt(numKeys));
            int value = random.nextInt(numValuesPerKeys);
            outport.emit(new KeyValPair<String, Object>(key, value));
        }
		try
		{
			Thread.sleep(emitDelay);
		} catch (Exception e)
		{
		}
	}

    public int getNumKeys() {
        return numKeys;
    }

    public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    }

    public int getNumValuesPerKeys() {
        return numValuesPerKeys;
    }

    public void setNumValuesPerKeys(int numValuesPerKeys) {
        this.numValuesPerKeys = numValuesPerKeys;
    }

    public int getTuppleBlast() {
        return tuppleBlast;
    }

    public void setTuppleBlast(int tuppleBlast) {
        this.tuppleBlast = tuppleBlast;
    }

    public int getEmitDelay() {
        return emitDelay;
    }

    public void setEmitDelay(int emitDelay) {
        this.emitDelay = emitDelay;
    }
}
