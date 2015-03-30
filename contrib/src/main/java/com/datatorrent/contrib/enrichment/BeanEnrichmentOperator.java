package com.datatorrent.contrib.enrichment;


import com.datatorrent.api.Context;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BeanEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> {

    Class inputClass;
    Class outputClass;
    List<Field> fields = new ArrayList();
    private List<Field> updates = new ArrayList<Field>();

    @Override
    protected Object getKey(Object tuple) {
        ArrayList<Object> keyList = new ArrayList<Object>();
        for(Field field : fields) {
            try {
                keyList.add(field.get(tuple));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return keyList;
    }

    @Override
    protected Object convert(Object in, Object cached) {
        Object o = null;
        if (cached == null)
            return in;

        Map<String, Object> newAttributes = (Map<String, Object>)cached;
        try {
            o = outputClass.newInstance();

            // TODO copy fields from input to output.

            for(Field f : updates) {
                f.set(o, newAttributes.get(f.getName()));
            }

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return o;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        for (String fName : lookupFields) {
            try {
                Field f = inputClass.getField(fName);
                fields.add(f);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }


        for (String fName : includeFields) {
            try {
                Field f = outputClass.getField(fName);
                updates.add(f);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }


    }
}
