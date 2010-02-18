/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lwes.hadoop.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.*;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.NoSuchAttributeException;
import org.lwes.hadoop.EventWritable;

/**
 * Adapter to use Journal Files in Hive.
 * Journal files may either be stored as a stream of EventWritable or
 * EventListWritable, SerDe will detect that through introspection.
 *
 * SerDe maps hive columns to event fields. Since hive columns are lowercase
 * but event fields may be uppercase, or we may want to call the hive column
 * with a name more descriptive than the event field, mapping can be provided
 * when creating the table, or later, using SERDEPROPERTIES setting
 *  'hive-column-name' = 'LWES-field-name'. If mapping is not provided,
 * SerDe will assume that the field name is the same as the column name.
 *
 * When using EventWritable, the event type in the file must be specified
 * using (example)
 *  'lwes.event_name'='Impression::Confirmed'
 *
 * When using a EventListWritable based journal, the event name must also
 * be specified in the column mapping, so one mapping would be
 *
 *  'sender_ip'='Impression::Confirmed::SenderIP'
 *
 * @author rcongiu
 */
public class EventSerDe implements SerDe {

    public static final Log LOG = LogFactory.getLog(EventSerDe.class.getName());
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    TypeInfo rowTypeInfo;
    ObjectInspector rowObjectInspector;
    boolean[] columnSortOrderIsDesc;
    // holds the results of deserialization
    ArrayList<Object> row;
    Map<String, List<FieldAndPosition>> fieldsForEventName =
            new HashMap<String, List<FieldAndPosition>>();
    String allEventName; // in case file has one type of event only

    /**
     * Prepares for serialization/deserialization, collecting the column
     * names and mapping them to the LWES attributes they refer to.
     * 
     * @param conf
     * @param tbl
     * @throws SerDeException
     */
    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        LOG.debug("initialize, logging to " + EventSerDe.class.getName());

        // Get column names and sort order
        String columnNameProperty = tbl.getProperty("columns");
        String columnTypeProperty = tbl.getProperty("columns.types");
        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }
        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }
        assert (columnNames.size() == columnTypes.size());

        for (String s : tbl.stringPropertyNames()) {
            LOG.debug("Property: " + s + " value " + tbl.getProperty(s));
        }

        if (tbl.containsKey("lwes.event_name")) {
            allEventName = tbl.getProperty("lwes.event_name");
        }

        // Create row related objects
        rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        //rowObjectInspector = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        rowObjectInspector = (StructObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
        row = new ArrayList<Object>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            row.add(null);
        }

        // Get the sort order
        String columnSortOrder = tbl.getProperty(Constants.SERIALIZATION_SORT_ORDER);
        columnSortOrderIsDesc = new boolean[columnNames.size()];
        for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
            columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder.charAt(i) == '-');
        }

        // take each column and find what it maps to into the event list
        int colNumber = 0;
        for (String columnName : columnNames) {
            String fieldName;
            String eventName;

            if (!tbl.containsKey(columnName) && allEventName == null) {
                LOG.warn("Column " + columnName
                        + " is not mapped to an eventName:field through SerDe Properties");
                continue;
            } else if (allEventName != null) {
                // no key, but in a single-type event file
                eventName = allEventName;
                fieldName = columnName;
            } else {
                // if key is there
                String fullEventField = tbl.getProperty(columnName);
                String[] parts = fullEventField.split("::");

                // we are renaming the column
                if (parts.length < 1 || (parts.length == 1 && allEventName != null)) {
                    LOG.warn("Malformed EventName::Field " + fullEventField);
                    continue;
                } else if (parts.length == 1 && allEventName != null) {
                    // adds the name. We're not specifying the event.
                    fieldName = parts[0];
                    eventName = allEventName;
                } else {
                    fieldName = parts[parts.length - 1];
                    eventName = fullEventField.substring(0, fullEventField.length() - 2 - fieldName.length());
                }
                LOG.debug("Mapping " + columnName + " to EventName " + eventName
                        + ", field " + fieldName);
            }

            if (!fieldsForEventName.containsKey(eventName)) {
                fieldsForEventName.put(eventName, new LinkedList<FieldAndPosition>());
            }

            fieldsForEventName.get(eventName).add(new FieldAndPosition(fieldName, colNumber));
            colNumber++;
        }



    }

    /**
     * Deserializes a single event, storing its fields into the row object
     * @param ev
     */
    @Override
    public Object deserialize(Writable w) throws SerDeException {
        byte[] bytes = null;

        LOG.debug("Deserialize called.");

        Event ev = null;
        try {
            if (w instanceof BytesWritable) {
                BytesWritable b = (BytesWritable) w;
                ev = new Event(b.getBytes(), false, null);
            } else if (w instanceof EventWritable) {
                EventWritable ew = (EventWritable) w;
                ev = ew.getEvent();
            } else {
                throw new SerDeException(getClass().toString()
                        + ": expects either BytesWritable or Text object!");
            }
            LOG.debug("Got bytes: " + bytes);
        } catch (EventSystemException ex) {
            throw new SerDeException(ex);
        }

        if (this.fieldsForEventName.containsKey(ev.getEventName())) {
            for (FieldAndPosition fp : fieldsForEventName.get(ev.getEventName())) {

                TypeInfo type = columnTypes.get(fp.getPosition());

                LOG.debug("Deserializing " + columnNames.get(fp.getPosition()));
                try {
                    row.set(fp.getPosition(),
                            deserialize_column(ev, type, fp,
                            row.get(fp.getPosition()))); // reusable object
                } catch (IOException ex) {
                    row.set(fp.getPosition(), null);
                    Logger.getLogger(EventSerDe.class.getName()).log(Level.SEVERE, null, ex);
                    continue;
                }
            }
        }
        return row;
    }

    /**
     * Takes the parsed event, and maps its content to columns.
     * @param ev
     * @param type
     * @param invert
     * @param reuse
     * @return
     * @throws IOException
     */
    static Object deserialize_column(Event ev, TypeInfo type,
            FieldAndPosition fp, Object reuse) throws IOException {
        LOG.debug("Deserializing column " + fp.getField());
        // Is this field a null?
        String fieldName = fp.getField();

        // if field is not there or is not set, return null.
        // isSet doesn't throw AttributeNotExists exception.
        if (!ev.isSet(fieldName)) {
            return null;
        }
        try {
            switch (type.getCategory()) {
                case PRIMITIVE: {
                    PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
                    switch (ptype.getPrimitiveCategory()) {
                        case VOID: {
                            return null;
                        }
                        case BOOLEAN: {
                            BooleanWritable r = reuse == null ? new BooleanWritable() : (BooleanWritable) reuse;
                            r.set(ev.getBoolean(fieldName));
                            return r;
                        }
                        case BYTE: {
                            throw new IOException("BYTE not supported. Use int instead");
                        }
                        case SHORT: {
                            ShortWritable r = reuse == null ? new ShortWritable() : (ShortWritable) reuse;
                            r.set(ev.getInt16(fieldName));
                            return r;
                        }
                        case INT: {
                            IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
                            r.set(ev.getInt32(fieldName));
                            return r;
                        }
                        case LONG: {
                            LongWritable r = reuse == null ? new LongWritable() : (LongWritable) reuse;
                            r.set(ev.getInt64(fieldName));
                            return r;
                        }
                        // support float through conversion from string since LWES does
                        // not support floats
                        case FLOAT: {
                            FloatWritable r = reuse == null ? new FloatWritable() : (FloatWritable) reuse;
                            r.set(Float.parseFloat(ev.getString(fieldName)));
                            return r;
                        }
                        case DOUBLE: {
                            DoubleWritable r = reuse == null ? new DoubleWritable() : (DoubleWritable) reuse;
                            r.set(Double.parseDouble(ev.getString(fieldName)));
                            return r;
                        }
                        case STRING: {
                            Text r = reuse == null ? new Text() : (Text) reuse;
                            // this will work for String nd IPAddress type.
                            r.set(ev.get(fieldName).toString());
                            return r;
                        }
                        default: {
                            throw new RuntimeException("Unrecognized type: " + ptype.getPrimitiveCategory());
                        }
                    }
                }
                case LIST:
                case MAP:
                case STRUCT: {
                    throw new IOException("List, Map and Struct not supported in LWES");
                }
                default: {
                    throw new RuntimeException("Unrecognized type: " + type.getCategory());
                }
            }
        } catch (NoSuchAttributeException ex) {
            // we check for the attribute existence beforehead with isSet
            // so we should never be here. Let's just repackage the exception.
            throw new IOException(ex);
        }

    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        LOG.debug("JournalSerDe::getObjectInspector()");
        return rowObjectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        LOG.debug("JournalSerDe::getSerializedClass()");
        return BytesWritable.class;
    }
    /**
     * Serializes an event to a writable object that can be handled by hadoop
     * @param obj
     * @param inspector
     * @return
     * @throws SerDeException
     */

    // buffers used to serialize
    // we reused these objects since we don't want to create/destroy
    // a huge number of objects in the JVM
    EventWritable serialized = new EventWritable();
    Event serializeev = null;

    @Override
    public Writable serialize(Object obj, ObjectInspector inspector) throws SerDeException {
        LOG.debug("Serializing: " + obj.getClass().getCanonicalName());

        try {
            if(serializeev == null)
                serializeev = new Event(allEventName, false, null);
        } catch (EventSystemException ex) {
            LOG.debug("Cannot create event ", ex);
            throw new SerDeException("Can't create event in SerDe serialize", ex);
        }

        StructObjectInspector soi = (StructObjectInspector) inspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();

        // for each field supplied by object inspector
        for (int i = 0; i < fields.size(); i++) {
            // for each column, set the correspondent event attribute
            String fieldName = fields.get(i).getFieldName();
            try {
                serialize_column(serializeev,
                        columnNames.get(i),
                        soi.getStructFieldData(obj, fields.get(i)), //data for Struct Field
                        fields.get(i).getFieldObjectInspector());  // object inspector
            } catch (EventSystemException ex) {
                LOG.debug("Cannot set field " + fieldName, ex);
                throw new SerDeException("Can't set field " + fieldName
                        + " in SerDe serialize", ex);
            }
        }
        // if u ask me, this should be BytesWritable
        // but apparently hive gets confused
        serialized.setEvent(serializeev);
        
        return serialized;

    }

    private void serialize_column(Event ev, String f, Object o, ObjectInspector oi)
            throws EventSystemException {
        LOG.debug("Serializing column" + f);

        // just don't set the field.
        if (o == null) {
            return;
        }

        switch (oi.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case VOID: {
                        return;
                    }
                    case BOOLEAN: {
                        BooleanObjectInspector boi = (BooleanObjectInspector) poi;
                        boolean v = ((BooleanObjectInspector) poi).get(o);
                        ev.setBoolean(f, v);
                        return;
                    }
                    case BYTE: {
                        ByteObjectInspector boi = (ByteObjectInspector) poi;
                        byte v = boi.get(o);
                        // lwes doesn't have byte so we upcast
                        ev.setInt16(f, v);
                        return;
                    }
                    case SHORT: {
                        ShortObjectInspector spoi = (ShortObjectInspector) poi;
                        short v = spoi.get(o);
                        ev.setInt16(f, v);
                        return;
                    }
                    case INT: {
                        IntObjectInspector ioi = (IntObjectInspector) poi;
                        int v = ioi.get(o);
                        ev.setInt32(f, v);
                        return;
                    }
                    case LONG: {
                        LongObjectInspector loi = (LongObjectInspector) poi;
                        long v = loi.get(o);
                        ev.setInt64(f, v);
                        return;
                    }
                    case FLOAT: {
                        FloatObjectInspector foi = (FloatObjectInspector) poi;
                        float v = foi.get(o);
                        ev.setString(f, Float.toString(v));
                        return;
                    }
                    case DOUBLE: {
                        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
                        double v = doi.get(o);
                        ev.setString(f, Double.toString(v));
                        return;
                    }
                    case STRING: {
                        StringObjectInspector soi = (StringObjectInspector) poi;
                        Text t = soi.getPrimitiveWritableObject(o);
                        ev.setString(f, (t != null ? t.toString() : null));
                        return;
                    }
                    default: {
                        throw new RuntimeException("Unrecognized type: " + poi.getPrimitiveCategory());
                    }
                }
            }
            case LIST:
            case MAP:
            case STRUCT: {
                throw new RuntimeException("Complex types not supported in LWES: " + oi.getCategory());
            }
            default: {
                throw new RuntimeException("Unrecognized type: " + oi.getCategory());
            }
        }
    }

    public static class FieldAndPosition {

        String field;
        int position;

        public FieldAndPosition(String f, int p) {
            field = f;
            position = p;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        @Override
        public String toString() {
            return "<" + field + "," + position + ">";
        }
    }
}
