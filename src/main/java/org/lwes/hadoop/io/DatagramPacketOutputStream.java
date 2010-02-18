/*
 * Serializes an event as a Datagram Packet
 */
package org.lwes.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.lwes.Event;
import org.lwes.NoSuchAttributeException;
import org.lwes.journaller.DeJournaller;
import org.lwes.journaller.util.EventHandlerUtil;

import org.lwes.serializer.Serializer;
import org.lwes.util.IPAddress;

class DatagramPacketOutputStream extends DataOutputStream {

    static int HEADER_LENGTH = 22;
    static IPAddress default_ip = new IPAddress();

    public DatagramPacketOutputStream(OutputStream os)
            throws IOException {
        super(os);
    }

    /**
     * Write a datagram to an output stream as follows
     *
     * 1) 2 bytes payload length (length of the datagram)
     * 2) 8 bytes receipt time (output of java.lang.System.currentTimeMillis())
     * 3) 6 bytes of sender's IP address:port (4 byte IP, 2 byte port)
     * 4) 2 bytes site ID
     * 5) 4 bytes of future extensions
     * 6) payload (Serialized event as described by Event.serialize())
     *
     */
    public void writeDataGram(DatagramPacket datagram, long receiptTime,
            int site_id) {
        try {
            int length = datagram.getLength();
            byte[] bytes = datagram.getData();
            byte[] header = new byte[HEADER_LENGTH];
            int offset = 0;

            /* payload length */
            offset += Serializer.serializeUINT16(length, header, offset);

            /* receipt Time */
            offset +=
                    Serializer.serializeINT64(receiptTime, header, offset);

            /* sender ip */
            offset +=
                    Serializer.serializeIPADDR(datagram.getAddress(), header, offset);

            /* sender port */
            offset += Serializer.serializeUINT16(datagram.getPort(), header, offset);

            /* site_id */
            offset += Serializer.serializeUINT16(site_id, header, offset);

            /* pad with zeros */
            offset += Serializer.serializeUINT32(0, header, offset);

            write(header, 0, HEADER_LENGTH);
            write(bytes, 0, length);
        } catch (Exception e) {
            System.err.println("Problem with OutputStreamFromDataGrams " + e);
        }
    }

    /**
     * Writes event to file using the same format
     * as @link{DatagramPacketInputStream#readEvent}
     * 
     * @param event
     * @throws IOException
     */
    public void writeEvent(Event event) throws IOException {
        byte[] packet = event.serialize();
 
        /*
         * these fields are in the datagram packet and will be always carried
         * over, unless they were already stripped from the original journal
         * ot the file we're reading is not a DatagramPacket Format
         */
        long receiptTime = 0;
        InetAddress senderIP = InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
        int senderPort = 0;
        int siteID = 0;

        // instead of checking for each one, just do a try catch
        try {
            if(event != null && event.isSet("ReceiptTime")) {
                receiptTime = event.getInt64("ReceiptTime");
                senderIP = InetAddress.getByAddress(event.getIPAddress("SenderIP"));
                senderPort = event.getUInt16("SenderPort");
                siteID = event.getUInt16("SiteID");
            }
        } catch (NoSuchAttributeException ex) {
            // just ignore... this may be a lwes non-journal file.
        }

        ByteBuffer b = ByteBuffer.allocate(DeJournaller.MAX_HEADER_SIZE);
        
        EventHandlerUtil.writeHeader(packet.length,
                receiptTime,
                senderIP,
                senderPort,
                siteID, b);
      
        out.write(b.array(), 0, DeJournaller.MAX_HEADER_SIZE);
        out.write(packet);
        out.flush();
    }
}
