/*
 * Copyright (C) 2013-2017, Levyx, Inc.
 *
 *  NOTICE:  All information contained herein is, and remains the property of
 *  Levyx, Inc.  The intellectual and technical concepts contained herein are
 *  proprietary to Levyx, Inc. and may be covered by U.S. and Foreign Patents,
 *  patents in process, and are protected by trade secret or copyright law.
 *  Dissemination of this information or reproduction of this material is
 *  strictly forbidden unless prior written permission is obtained from Levyx,
 *  Inc.  Access to the source code contained herein is hereby forbidden to
 *  anyone except current Levyx, Inc. employees, managers or contractors who
 *  have executed Confidentiality and Non-disclosure agreements explicitly
 *  covering such access.
 */

package com.levyx.xenon.util;

import com.levyx.xenon.*;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Vector;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static com.levyx.xenon.XenonClient.DatasetOpenMode.XE_O_READONLY;

public class XenonReader {
    private String host;
    private Integer port;
    private Integer bufferSize;
    private ForkJoinPool pool;
    XenonClient.DatasetStatus info;

    public XenonReader(String host, Integer port, Integer bufferSize) {
        this.host = host;
        this.port = port;
        this.bufferSize = bufferSize;
        pool = new ForkJoinPool();
    }

    private class XenonReadTask extends RecursiveAction {
        private Integer partStart;
        private Integer partEnd;
        private Vector<Vector<String>> result;
        private String datasetName;

        XenonReadTask(Integer partStart,
                      Integer partEnd,
                      Vector<Vector<String>> result,
                      String datasetName) {
            this.partStart = partStart;
            this.partEnd = partEnd;
            this.result = result;
            this.datasetName = datasetName;
        }

        private void directRead(Integer part) throws IOException {
            appendToVector(result.get(part), datasetName, part);
        }

        @Override
        protected void compute() {
            try {
                if (partStart.equals(partEnd)) {
                    directRead(partStart);
                } else if (partStart < partEnd) {
                    Integer partMid = (partStart + partEnd) >>> 1;
                    XenonReadTask l = new XenonReadTask(partStart, partMid, result, datasetName);
                    XenonReadTask r = new XenonReadTask(partMid + 1, partEnd, result, datasetName);
                    invokeAll(l, r);
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }


    public Vector<String> readFromXenon(String datasetName) throws IOException {
        Long numParts = getNumParts(datasetName);

        // Parallel
        Vector<Vector<String>> vectors = new Vector<>();
        for (Integer i = 0; i < numParts; i++) vectors.add(new Vector<>());

        Vector<String> result = new Vector<>();

        XenonReadTask job = new XenonReadTask(0, numParts.intValue() - 1, vectors, datasetName);
        pool.invoke(job);

        for (Vector<String> v : vectors) {
            result.addAll(v);
        }

        pool.shutdown();

        result.sort(Comparator.naturalOrder());

        // Sequential
        /*
        for (Integer i = 0; i < numParts; i++) {
            appendToVector(result, datasetName, i);
        }
        */

        Socket socket = new Socket(host, port);
        XenonClient client = new XenonClient(socket);
        client.connect();

        client.datasetClose(info.handler);
        client.disconnect();

        return result;
    }

    private Long getNumParts(String datasetName) throws IOException {
        Socket socket = new Socket(host, port);
        XenonClient client = new XenonClient(socket);
        client.connect();

        info = client.datasetOpen(
                datasetName,
                "",
                new XenonClient.DatasetOpenMode[]{XE_O_READONLY},
                0);

        client.disconnect();
        return info.fanout;
    }

    private void appendToVector(Vector<String> v, String datasetName, Integer partition) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        Socket socket = new Socket(host, port);
        XenonClient client = new XenonClient(socket);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
        client.connect();
        String[] fields = getFields(info.schema);
        Long rowsRead = -1L;
        while (rowsRead != 0) {
            rowsRead = client.datasetGet(info.handler, partition, buffer);
            for (Long unprocessed = rowsRead; unprocessed > 0; unprocessed--) {
                String line = getNextLine(buffer, fields);
                v.add(line);
            }
        }

        client.disconnect();
    }


    /**
     * Method to get fields from datasetSchema.
     *
     * @param schema - Schema when we open dataset in Read mode.
     * @return Array of fields returned after parsing schema.
     */

    private String[] getFields(String schema) {
        String[] fields = schema.substring(1, schema.length() - 1)
                .split(",(?![^\\[]*\\])");
        for (Integer i = 0; i < fields.length; i++) {
            String[] segments = fields[i].split(":");
            switch (segments[1]) {
                case "CHAR":
                    fields[i] = "String";
                    break;
                default:
                    fields[i] = fields[i].split(":")[1];
            }
        }

        return fields;
    }

    /**
     * Method to get next record.
     *
     * @param buffer - ByteBuffer to read from.
     * @param fields - Array of fields returned from dataset schema.
     * @return result - String of values, comma separated ,corresponding to each field type.
     */

    private String getNextLine(ByteBuffer buffer, String[] fields) {
        StringBuilder result = new StringBuilder();
        for (String type : fields) {
            if (result.length() != 0) {
                result.append(", ");
            }
            result.append(getFieldData(buffer, type));
        }
        return result.toString();
    }


    /**
     * Method to get field data.
     *
     * @param buffer - ByteBuffer to read from.
     * @param type   - Field type.
     * @return result - field data as string.
     */
    private String getFieldData(ByteBuffer buffer, String type) {
        String result;
        if (buffer.get() == 1) {
            switch (type) {
                case "BOOL":
                    result = Boolean.toString(buffer.get() != 0);
                    break;
                case "I8":
                    result = Byte.toString(buffer.get());
                    break;
                case "I16":
                    result = Short.toString(buffer.getShort());
                    break;
                case "I32":
                    result = Integer.toString(buffer.getInt());
                    break;
                case "I64":
                    result = Long.toString(buffer.getLong());
                    break;
                case "FLOAT":
                    result = Float.toString(buffer.getFloat());
                    break;
                case "DOUBLE":
                    result = Double.toString(buffer.getDouble());
                    break;
                case "String":
                    result = getString(buffer);
                    break;
                default:
                    if (type.startsWith("DECIMAL")) {
                        result = getDecimal(type, buffer);
                    } else {
                        result = "Error: Type not supported";
                    }
            }
        } else {
            result = "null";
        }

        return result;
    }


    /**
     * Method returns bytes stored in buffer as String .
     *
     * @param buffer - byteBuffer to read from.
     * @return String value stored in the buffer.
     */

    public String getString(ByteBuffer buffer) {
        Short strLen = buffer.getShort();
        if (strLen == 0) return "";
        else {
            byte[] bytes = new byte[strLen];
            buffer.get(bytes, 0, strLen);
            return new String(bytes);
        }
    }

    /**
     * Method returns decimal as String from buffer.
     *
     * @param type   - Name of the field.
     * @param buffer - byteBuffer to read from.
     * @return Decimal value as String.
     */
    private String getDecimal(String type, ByteBuffer buffer) {
        String[] attrs = type.substring(8, type.length() - 1).split(",");
        Integer precision = Integer.valueOf(attrs[0]);
        Integer scale = Integer.valueOf(attrs[1]);

        String unscaled;
        if (precision <= 9) {
            unscaled = Integer.toString(buffer.getInt());
        } else if (precision <= 18) {
            unscaled = Long.toString(buffer.getLong());
        } else {
            Integer numBytes = (precision / 9) * 4;
            byte[] tmpBuffer = new byte[numBytes];
            buffer.get(tmpBuffer, 0, numBytes);
            unscaled = new BigInteger(tmpBuffer).toString();
        }

        Integer dotPos = unscaled.length() - scale;
        if (dotPos <= 0) {
            String filling = "";
            for (Integer i = dotPos; i < 0; i++) filling = filling + "0";
            return "0." + filling + unscaled;
        } else {
            return unscaled.substring(0, dotPos) + "."
                    + unscaled.substring(dotPos, unscaled.length());
        }
    }

}


