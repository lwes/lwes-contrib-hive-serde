package org.lwes.hadoop.io;


import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.lwes.serializer.DeserializerState;
import org.lwes.serializer.Deserializer;
import org.lwes.Event;
import org.lwes.EventFactory;
import org.lwes.util.IPAddress;

public class DatagramPacketInputStream extends DataInputStream
{
  static int   HEADER_LENGTH    = 22;
  SimpleDateFormat dateFormatter = new SimpleDateFormat();
  private EventFactory ef      = null;
  private boolean typeChecking = false;  /* act as it did before */

  public void setEventFactory(EventFactory eventFactory)
    { this.ef = eventFactory; }
  public EventFactory getEventFactory()
    { return ef; }

  public void setTypeChecking(boolean typeChecking)
    { this.typeChecking = typeChecking; }
  public boolean isTypeChecking()
    { return typeChecking; }

  public DatagramPacketInputStream(InputStream is)
    throws IOException, StreamCorruptedException
  {
    super(is);
  }

  public Event readEvent()
  {
    Event event = null;

    if ( ef == null )
    {
      ef = new EventFactory();
    }
    try {
      DeserializerState myState = new DeserializerState();

      byte[] header = new byte[HEADER_LENGTH];
      readFully(header,0,HEADER_LENGTH);
      int length = (int)(Deserializer.deserializeUINT16(myState,header));
      long time = Deserializer.deserializeINT64(myState,header);
      IPAddress source_inet =
        new IPAddress(Deserializer.deserializeIPADDR(myState,header));
      int    source_port =
        (int)(Deserializer.deserializeUINT16(myState,header));

      int  site_id =
          (int)(Deserializer.deserializeUINT16(myState,header));

      byte [] bytes = new byte[length];
      readFully(bytes,0,length);
      if ( isTypeChecking() )
      {
        event = ef.createEvent(bytes);
      }
      else
      {
          // create event without checks
        event = new Event(bytes,false,null);
      }
      event.setInt64("ReceiptTime",time);
      event.setIPAddress("SenderIP",source_inet);
      event.setUInt16("SenderPort",source_port);
      event.setUInt16("SiteID",site_id);
    } catch ( EOFException e ) {
      event=null;
    } catch ( Exception e ) {
      System.err.println("Problem with OutputStreamFromDataGrams "+e);
      e.printStackTrace();
      event=null;
    }
    return event;
  }

  public byte[] readDataGramBytes()
  {
    byte [] bytes = null;
    try {
      DeserializerState myState = new DeserializerState();

      byte[] header = new byte[HEADER_LENGTH];
      readFully(header,0,HEADER_LENGTH);
      int length = (int)(Deserializer.deserializeUINT16(myState,header));
      long time = Deserializer.deserializeINT64(myState,header);
      IPAddress source_inet =
        new IPAddress(Deserializer.deserializeIPADDR(myState,header));
      int    source_port =
        (int)(Deserializer.deserializeUINT16(myState,header));
      int  site_id =
          (int)(Deserializer.deserializeUINT16(myState,header));

      bytes = new byte[length];
      readFully(bytes,0,length);
    } catch ( EOFException e ) {
    } catch ( Exception e ) {
      System.err.println("Problem with OutputStreamFromDataGrams "+e);
    }
    return bytes;
  }

  public String readDataGram()
  {
    StringBuffer sb = new StringBuffer();
    int total = -1;
    try {
      DeserializerState myState = new DeserializerState();

      byte[] header = new byte[HEADER_LENGTH];
      total = read(header,0,HEADER_LENGTH);

      if ( total != -1 )
      {
        int length = (int)(Deserializer.deserializeUINT16(myState,header));
        long time = Deserializer.deserializeINT64(myState,header);
        IPAddress source_inet =
          new IPAddress(Deserializer.deserializeIPADDR(myState,header));
        int    source_port =
          (int)(Deserializer.deserializeUINT16(myState,header));
        int  site_id =
          (int)(Deserializer.deserializeUINT16(myState,header));

        dateFormatter.applyPattern("MM/dd/yyyy HH:mm:ss.SSS");
        Date date = new Date(time);
        sb.append(dateFormatter.format(date));

        byte [] bytes = new byte[length];
        total = read(bytes,0,length);
      }

    } catch ( Exception e ) {
      System.err.println("Problem with OutputStreamFromDataGrams "+e);
      e.printStackTrace();
    }
    if ( total == -1 ) return null;

    return sb.toString();
  }

}

