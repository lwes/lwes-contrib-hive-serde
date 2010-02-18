package org.lwes.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;
import org.lwes.Event;
import org.lwes.EventSystemException;
import org.lwes.NoSuchAttributeException;

/**
 * Writable wrapper around LWES events.
 * 
 * @author rcongiu
 */

public class EventWritable implements WritableComparable<EventWritable> {
	private Event event;

	public EventWritable() {
	}

	public EventWritable(Event event) {
		setEvent(event);
	}

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		final int length = in.readInt();
		final byte[] bytes = new byte[length];
		in.readFully(bytes);
                try {
                    setEvent(new Event(bytes, false, null));
                } catch (EventSystemException ex) {
                    throw new IOException("EventSystemException", ex);
                }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		final byte[] bytes = event.serialize();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public int compareTo(EventWritable o) {
		Thread.dumpStack();
		return compare(getEvent(), o.getEvent());
	}

	@SuppressWarnings("unchecked")
	private static int compare(Event a, Event b) {
		int comp = a.getEventName().compareTo(b.getEventName());
		if (comp!=0) return comp;
		comp = a.size()-b.size();
		if (comp!=0) return comp;
		final Set<String> anames = getAttributeNames(a), bnames = getAttributeNames(b);
		// Compare attribute names
		for (Iterator<String> ai = anames.iterator(), bi = bnames.iterator(); ai.hasNext()
		                                                                      ||bi.hasNext();) {
			final String aa = (ai.hasNext() ? ai.next() : ""), ba = (bi.hasNext() ? bi.next() : "");
			comp = aa.compareTo(ba);
			if (comp!=0) return comp;
		}
		// Compare attribute values
		for (String name : anames) {
			try {
				final Object av = a.get(name), bv = b.get(name);
				if (av==null&&bv==null) continue;
				if (av==null) return -1;
				if (bv==null) return 1;
				if ((av instanceof Comparable)&&(bv instanceof Comparable)) {
					comp = ((Comparable) av).compareTo(bv);
					if (comp!=0) return comp;
				} else {
					throw new IllegalArgumentException("Field "+name+" was not Comparable:\n\t"+a+"\n\t"+b);
				}
			} catch (NoSuchAttributeException e) {
				throw new IllegalArgumentException("Field "+name+" seemed to disappear!\n\t"+a+"\n\t"+b);
			}
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	private static Set<String> getAttributeNames(Event event) {
		final TreeSet<String> names = new TreeSet<String>();
		for (Enumeration<String> e = event.getEventAttributeNames(); e.hasMoreElements();)
			names.add(e.nextElement());
		return names;
	}

	@Override
	public String toString() {
		return event==null ? "null" : event.toString();
	}
}