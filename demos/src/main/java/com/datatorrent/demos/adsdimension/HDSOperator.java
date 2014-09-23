package com.datatorrent.demos.adsdimension;

import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

interface TimeStampedAggregate extends DimensionsComputation.AggregateEvent {
  public void setTimestamp(long timestamp);
  public long getTimestamp();
}

abstract public class HDSOperator<EVENT, AGGREGATE extends TimeStampedAggregate> extends AbstractSinglePortHDSWriter<AGGREGATE>
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSOutputOperator.class);

  protected boolean debug = false;

  protected final SortedMap<Long, Map<AGGREGATE, AGGREGATE>> cache = Maps.newTreeMap();

  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private DimensionsComputation.Aggregator<EVENT, AGGREGATE> aggregator;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  @Override
  public void endWindow()
  {
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<AGGREGATE, AGGREGATE> vals = cache.remove(cache.firstKey());
      for (Map.Entry<AGGREGATE, AGGREGATE> en : vals.entrySet()) {
        AGGREGATE ai = en.getValue();
        try {
          super.processEvent(ai);
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }
    super.endWindow();
  }

  @Override protected void processEvent(AGGREGATE aggr) throws IOException
  {
    Map<AGGREGATE, AGGREGATE> valMap = cache.get(aggr.getTimestamp());
    if (valMap == null) {
      valMap = new HashMap<AGGREGATE, AGGREGATE>();
      valMap.put(aggr, aggr);
      cache.put(aggr.getTimestamp(), valMap);
    } else {
      AGGREGATE val = valMap.get(aggr);
      if (val == null) {
        valMap.put(aggr, aggr);
        return;
      } else {
        aggregator.aggregate(val, aggr);
      }
    }
  }

  public DimensionsComputation.Aggregator getAggregator()
  {
    return aggregator;
  }


  public void setAggregator(DimensionsComputation.Aggregator aggregator)
  {
    this.aggregator = aggregator;
  }


  public boolean isDebug()
  {
    return debug;
  }

  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }
}
