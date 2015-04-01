package com.datatorrent.contrib.enrichment;


import com.datatorrent.api.Context;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class BeanEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> {

  public Class inputClass;
  public Class outputClass;
  private List<Field> fields = new ArrayList<Field>();
  private List<Field> updates = new ArrayList<Field>();

  @Override
  protected Object getKey(Object tuple) {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(Field f: fields) {
      try {
        keyList.add(f.get(tuple));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return keyList;
  }

  @Override
  protected Object convert(Object in, Object cached) {
    Object o = null;
    if (cached == null)
      return in;

    ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
    try {
      o = outputClass.newInstance();

      // TODO copy fields from input to output.

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
    }
  }

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    for (String fName : lookupFields) {
      try {
        Field f = inputClass.getField(fName);
        f.setAccessible(true);
        fields.add(f);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }


    for (String fName : includeFields) {
      try {
        Field f = outputClass.getField(fName);
        updates.add(f);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }


  }
}
