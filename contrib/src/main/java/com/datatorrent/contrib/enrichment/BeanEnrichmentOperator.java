package com.datatorrent.contrib.enrichment;

import com.datatorrent.api.Context;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * This class takes a POJO as input and extract the value of the lookupKey configured
 * for this operator. It then does a lookup in file/DB to find matching entry and all key-value pairs
 * specified in the file/DB or based on include fields are added to original tuple.
 *
 * Properties:<br>
 * <b>inputClass</b>: Class to be loaded for the incoming data type<br>
 * <b>outputClass</b>: Class to be loaded for the emitted data type<br>
 * <br>
 *
 * Example
 * The file contains data in json format, one entry per line. during setup entire file is read and
 * kept in memory for quick lookup.
 * If file contains following lines, and operator is configured with lookup key "productId"
 * { "productId": 1, "productCategory": 3 }
 * { "productId": 4, "productCategory": 10 }
 * { "productId": 3, "productCategory": 1 }
 *
 * And input tuple is
 * { amount=10.0, channelId=4, productId=3 }
 *
 * The tuple is modified as below before operator emits it on output port.
 * { amount=10.0, channelId=4, productId=3, productCategory=1 }
 *
 * @displayName BeanEnrichment
 * @category Database
 * @tags enrichment, lookup
 *
 * @since 2.1.0
 */
public class BeanEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> {

  private transient static final Logger logger = LoggerFactory.getLogger(BeanEnrichmentOperator.class);
  protected Class inputClass;
  protected Class outputClass;
  private transient List<Field> updates = new ArrayList<Field>();
  private transient List<GetterObject> getters = new ArrayList<GetterObject>();

  @Override
  protected Object getKey(Object tuple) {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(GetterObject g : getters) {
        keyList.add(g.get(tuple));
    }
    return keyList;
  }

  @Override
  protected Object convert(Object in, Object cached) {
    try {
      Object o = outputClass.newInstance();
      // Copy the fields from input to output
      Field[] fields = inputClass.getFields();

      for(Field f : fields) {
        outputClass.getField(f.getName()).set(o, f.get(in));
      }
      if (cached == null)
        return o;

      if(updates.size() == 0 && includeFields.size() != 0) {
        populateUpdatesFrmIncludeFields();
      }
      ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
      int idx = 0;
      for(Field f : updates) {
        f.set(o, newAttributes.get(idx));
        idx++;
      }
      return o;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    populateUpdatesFrmIncludeFields();
  }

  private void populateGettersFrmLookup()
  {
    for (String fName : lookupFields) {
        GetterObject f = PojoUtils.createGetterObject(inputClass, fName);
        getters.add(f);
    }
  }

  private void populateUpdatesFrmIncludeFields() {
    for (String fName : includeFields) {
      try {
        Field f = outputClass.getField(fName);
        f.setAccessible(true);
        updates.add(f);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void setOutputClass(String outputClass)
  {
    try {
      this.outputClass = this.getClass().getClassLoader().loadClass(outputClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override protected void processTuple(Object tuple)
  {
    if (inputClass == null) {
      inputClass = tuple.getClass();
      populateGettersFrmLookup();
    }
    super.processTuple(tuple);
  }
}
