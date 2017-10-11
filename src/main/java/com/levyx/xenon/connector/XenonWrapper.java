/**
 * THE SOFTWARE BELOW IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS OF
 * THE SOFTWARE BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 **/

package com.levyx.xenon.connector;

import com.levyx.xenon.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class XenonWrapper {
    private static final Logger log = LoggerFactory.getLogger(XenonWrapper.class);
    private boolean verbose = false;

    private XenonClient xenonClient;
    private ByteBuffer socketBuffer;
    private ByteBuffer tmpBuffer;
    private XenonClient.DatasetStatus datasetStatus;
    private String host;
    private String datasetName;
    private String schema;
    private int port;
    private Integer schemaVersion;

    XenonWrapper(Map<String, String> props) throws ConnectException {
        XenonSinkConnectorConfig config = new XenonSinkConnectorConfig(props);

        host = config.host;
        port = config.port;
        schemaVersion = config.schemaVersion;
        datasetName = config.datasetName;
        schema = config.datasetSchema;

        socketBuffer = ByteBuffer.allocate(config.bufferCapacity);
        Integer MAX_RECORD_LENGTH = 4096;
        tmpBuffer = ByteBuffer.allocate(MAX_RECORD_LENGTH);

        try {
            xenonClient = getXeClient();
        } catch (IOException e) {
            throw new ConnectException("Unable to create socket to Xenon");
        }
    }

    /**
     * Create connection to xenon.
     *
     * @return Object of class XenonClient.
     */
    private XenonClient getXeClient() throws ConnectException, IOException {
        xenonClient = new XenonClient(new Socket(host, port));
        if (!xenonClient.connect()) {
            throw new ConnectException("XeConnect failed");
        }
        return xenonClient;
    }

    /**
     * Saving records to xenon.
     */
    void saveRecords(Collection<SinkRecord> sinkRecords) {
        Integer recordLength;
        Long sid = -1L;
        if (sinkRecords.iterator().hasNext()) {
            sid = (long) sinkRecords.iterator().next().kafkaPartition() % datasetStatus.fanout;
        }

        for (SinkRecord record : sinkRecords) {
            recordLength = fillTempBuffer(record);
            //since Collection<sinkRecord> has same partitions for each task.
            if (socketBuffer.remaining() < recordLength) {
                xenonClient.datasetPut(datasetStatus.handler, sid, socketBuffer);
            }
            socketBuffer.put(tmpBuffer.array(), 0, recordLength);
        }

        if (socketBuffer.position() != 0) {
            xenonClient.datasetPut(datasetStatus.handler, sid, socketBuffer);
        }

    }

    /**
     * Open dataset.
     */
    void openDataSet() throws ConnectException {

        XenonClient.DatasetOpenMode[] createMode = {
                XenonClient.DatasetOpenMode.XE_O_CREATE,
                XenonClient.DatasetOpenMode.XE_O_WRITEONLY
        };

        try {
            datasetStatus = xenonClient.datasetOpen(
                    datasetName,
                    schema,
                    createMode,
                    0);
        } catch (IllegalArgumentException e) {
            xenonClient.disconnect();
            throw new ConnectException(e.getMessage());
        }
    }


    /**
     * Closing dataset.
     */
    void close() throws ConnectException {
        try {
            if (!xenonClient.datasetClose(datasetStatus.handler)) {
                throw new ConnectException("Unable to close Xenon dataset");
            }
            xenonClient.disconnect();
        } catch (IllegalArgumentException e) {
            throw new ConnectException(e.getMessage());
        }
    }


    /**
     * Handles schema and schemaless records. If with schema name then use that,
     * else use class that record value belongs to.
     *
     * @return schema as String.
     */
    private String getSchemaType(Schema schema, SinkRecord record) {
        if (schema == null || schema.name() == null) {
            return record.value().getClass().getSimpleName();
        } else if (schema.name().equals(Date.LOGICAL_NAME) ||
                schema.name().equals(Time.LOGICAL_NAME) ||
                schema.name().equals(Timestamp.LOGICAL_NAME) ||
                schema.name().equals(Decimal.LOGICAL_NAME)) {
            return schema.name();
        } else {
            return record.value().getClass().getSimpleName();
        }
    }


    /**
     * Filling the temporary Buffer with SinkRecords.
     *
     * @return recordLength.
     */
    private Integer fillTempBuffer(SinkRecord record) throws ConnectException {
        Schema schema = record.valueSchema();
        tmpBuffer.clear();

        if (record.value() == null) {
            fillSimpleObject(tmpBuffer, record.value(), "Null");
            return tmpBuffer.position();
        }

        String schemaType = getSchemaType(schema, record);

        switch (schemaType) {
            case "HashMap":
                processMap(record.value());
                break;

            case "ArrayList":
                processArray(record.value());
                break;

            case "Struct":
                processStruct(tmpBuffer, record.value(), schema);
                break;

            default:
                fillSimpleObject(tmpBuffer, record.value(), schemaType);

        }

        return tmpBuffer.position();
    }


    /**
     * Parse fields in record value by checking
     * and matching schema type.
     */
    private void processStruct(ByteBuffer buffer, Object x, Schema schema)
            throws ConnectException {
        try {
            if (schema.version().equals(schemaVersion)) {
                for (Field field : schema.fields()) {
                    if (field.schema().type() != null) {
                        /*Parse fields in record value and match with correct Schema type*/
                        switch (field.schema().type()) {

                            case BYTES:
                                byte[] valueArr = null;
                                if (((Struct) x).get(field) instanceof byte[]) {
                                    valueArr = (byte[]) ((Struct) x).get(field);
                                } else if ((((Struct) x).get(field) instanceof ByteBuffer)) {
                                    valueArr = ((ByteBuffer) ((Struct) x).get(field)).array();
                                }
                                if (valueArr == null) {
                                    throw new ConnectException("Invalid input for bytes type: "
                                            + ((Struct) x).get(field).getClass());

                                }
                                buffer.put((byte) 1);
                                buffer.putShort((short) valueArr.length);
                                buffer.put(valueArr);
                                if (verbose) {
                                    log.info("tmpBuffer components are ---> "
                                            + "" + Arrays.toString(buffer.array()));
                                }
                                break;


                            default:
                                Object value = ((Struct) x).get(field);
                                fillSimpleObject(tmpBuffer, value,
                                        value.getClass().getSimpleName());
                        }
                    }
                }
            } else {
                throw new ConnectException("Unmatched Schema version of record.");
            }
        } catch (ConnectException e) {
            e.printStackTrace();
        }
    }


    /**
     * Parse fields in record value of type Map
     * by checking and matching each field type.
     */
    private void processMap(Object x) throws ConnectException {
        if (verbose) {
            log.info("Schema in object:" + schema.substring(1, schema.length() - 1));
        }
        String[] fields = schema.substring(1, schema.length() - 1).split(", ");
        Map<?, ?> map = (Map<?, ?>) x;
        tmpBuffer.clear();
        for (String field : fields) {
            String fieldName = field.split(":")[0];
            Object value = map.get(fieldName);

            if (value == null) {
                throw new ConnectException("Invalid null value for required"
                        + "field in Map input");
            } else if (value instanceof Map) {
                throw new ConnectException("Unacceptable nested map value");
            } else {
                String fieldType = value.getClass().getSimpleName();
                fillSimpleObject(tmpBuffer, value, fieldType);
            }

        }
    }

    /**
     * Parse fields in record value of type Array
     * by checking and matching each field type.
     */
    private void processArray(Object x) throws ConnectException {
        Collection collection = (Collection) x;
        for (Object rec : collection) {
            if (rec != null && !(rec instanceof Collection)) {
                fillSimpleObject(tmpBuffer, rec, rec.getClass().getSimpleName());
            } else if (rec == null) {
                throw new ConnectException("Invalid null value for required field in Array input");
            } else {
                throw new ConnectException("Unacceptable nested array value");
            }
        }
    }

    /**
     * check and match record value
     * with the correct field type.
     */
    private void fillSimpleObject(ByteBuffer buffer, Object x, String type)
            throws ConnectException {
        if (verbose) {
            log.info("value of object is " + x);
        }
        switch (type) {
            case "String":
                byte[] stringArr = x.toString().getBytes();
                buffer.put((byte) 1);
                buffer.putShort((short) stringArr.length);
                buffer.put(stringArr);
                break;

            case "byte[]":
                byte[] valueArr = (byte[]) x;
                if (valueArr == null) {
                    throw new ConnectException("Invalid type for bytes type: " + x.getClass());
                }
                buffer.put((byte) 1);
                buffer.putShort((short) valueArr.length);
                buffer.put(valueArr);
                break;

            case "HeapByteBuffer":
                byte[] valArr = ((ByteBuffer) x).array();
                buffer.put((byte) 1);
                buffer.putShort((short) valArr.length);
                buffer.put(valArr);
                break;

            case "Boolean":
                buffer.put((byte) 1);
                boolean check = (Boolean) x;
                if (check) {
                    buffer.put((byte) 1);
                } else {
                    buffer.put((byte) 0);
                }
                break;

            case "Double":
                buffer.put((byte) 1);
                buffer.putDouble((Double) (x));
                break;

            case "Float":
                buffer.put((byte) 1);
                buffer.putFloat((Float) (x));
                break;

            case "Long":
                buffer.put((byte) 1);
                buffer.putLong((Long) (x));
                break;

            case "Integer":
                buffer.put((byte) 1);
                buffer.putInt((Integer) (x));
                break;

            case "Short":
                buffer.put((byte) 1);
                buffer.putShort((Short) (x));
                break;

            case "Byte":
                buffer.put((byte) 1);
                buffer.put((Byte) (x));
                break;

            case "Null":
                buffer.put((byte) 0);
                break;

            case Date.LOGICAL_NAME:
                buffer.put((byte) 1);
                buffer.putLong(((java.util.Date) x).getTime());
                break;

            case Time.LOGICAL_NAME:
                buffer.put((byte) 1);
                buffer.putLong(((java.util.Date) x).getTime());
                break;

            case Timestamp.LOGICAL_NAME:
                buffer.put((byte) 1);
                buffer.putLong(((java.util.Date) x).getTime());
                break;

            case Decimal.LOGICAL_NAME:
                BigInteger unscaledVal = ((BigDecimal) x).unscaledValue();
                int inputPrecision = ((BigDecimal) x).precision();
                List<Integer> seq = Arrays.asList(9, 18, 27, inputPrecision);
                Collections.sort(seq);
                int index = seq.indexOf(inputPrecision) + 1;
                switch (index) {
                    case 1:
                        buffer.put((byte) 1);
                        buffer.putInt(unscaledVal.intValue());
                        break;

                    case 2:
                        buffer.put((byte) 1);
                        buffer.putLong(unscaledVal.longValue());
                        break;

                    default:
                        byte[] decimalArr = unscaledVal.toByteArray();
                        int len = decimalArr.length;
                        buffer.put((byte) 1);
                        for (int i = 1; i <= 4 * index - len; i++) {
                            if (unscaledVal.compareTo(BigInteger.ZERO) >= 0) {
                                buffer.put((byte) 0);
                            } else {
                                buffer.put((byte) 0xFF);
                            }
                        }
                        buffer.put(decimalArr);
                }
                break;

            default:
                throw new ConnectException("unmatched Class type");
        }

        if (verbose) {
            log.info("DEBUG->tmpBuffer components are ---> "
                    + "" + Arrays.toString(buffer.array()));
        }
    }
}
