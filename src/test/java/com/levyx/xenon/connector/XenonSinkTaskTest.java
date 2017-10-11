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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.levyx.xenon.util.Version;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import static com.levyx.xenon.connector.XenonSinkConnectorConfig.*;

import com.levyx.xenon.util.XenonReader;
import com.levyx.xenon.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.EMPTY_MAP;
import static org.junit.Assert.*;


/**
 * XenonSinkTaskTest.java
 * Test and validate methods in XenonSinkTask.java
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(XenonSinkTask.class)
public class XenonSinkTaskTest {
    private static final Logger log = LoggerFactory.getLogger(XenonSinkTaskTest.class);
    private static final String TOPIC = "topic";
    private static final String SCHEMA_NAME = "schema";
    private static final String PAYLOAD_NAME = "payload";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonConverter converter = new JsonConverter();
    private XenonSinkTask task;
    private SinkTaskContext ctx;
    private Schema recSchema;

    @Before
    public void setUp() {
        task = new XenonSinkTask();
        ctx = PowerMock.createMock(SinkTaskContext.class);
        task.initialize(ctx);
        converter.configure(EMPTY_MAP, false);
    }

    /**
     * Validates version passed in the XenonSinkTask.java.
     */
    @Test
    public void testVersion() {
        PowerMock.replayAll();
        assertEquals(Version.getVersion(), task.version());
        PowerMock.verifyAll();
    }

    /**
     * Test pushing record to xenon with boolean schema and null payload.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalBooleanSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalBooleanSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
        Map<String, String> map = buildNullBoolean("testConnectAndOptionalBooleanSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalBooleanSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as float and payload null
     * Further toConnectData is validated for this case.
     * We then create a record with key as string and push record value to xenon.
     */

    @Test
    public void testConnectAndOptionalFloatSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalFloatSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"float\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_FLOAT32_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_FLOAT32_SCHEMA, null);
        Map<String, String> map = buildNullFloat("testConnectAndOptionalFloatSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalFloatSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as double and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalDoubleSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalDoubleSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"double\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_FLOAT64_SCHEMA, null);
        Map<String, String> map = buildNullDouble("testConnectAndOptionalDoubleSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalDoubleSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int8 and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalByteSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalByteSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int8\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_INT8_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_INT8_SCHEMA, null);
        Map<String, String> map = buildNullByte("testConnectAndOptionalByteSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalByteSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int16 and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalShortSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalShortSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int16\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_INT16_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_INT16_SCHEMA, null);
        Map<String, String> map = buildNullShort("testConnectAndOptionalShortSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalShortSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int32 and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalIntegerSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalIntegerSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int32\", \"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_INT32_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_INT32_SCHEMA, null);
        Map<String, String> map = buildNullInt("testConnectAndOptionalIntegerSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalIntegerSchema", record.value());
        client.disconnect();
    }


    /**
     * Test pushing record to xenon with schema as int64 and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalLongSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalLongSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int64\", "
                + "\"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_INT64_SCHEMA, null);
        Map<String, String> map = buildNullLong("testConnectAndOptionalLongSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalLongSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as bytes and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalBytesSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalBytesSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"bytes\", "
                + "\"optional\": true }, "
                + "\"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_BYTES_SCHEMA, null);
        Map<String, String> map = buildNullByteArrayByteBuffer("testConnectAndOptional"
                + "BytesSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalBytesSchema", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as string and payload null.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testConnectAndOptionalStringSchema() throws IOException {
        XenonClient client = checkExist("testConnectAndOptionalStringSchema");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"string\","
                + " \"optional\": true },"
                + " \"payload\": null }").getBytes();
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.OPTIONAL_STRING_SCHEMA, null);
        Map<String, String> map = buildNullByteArrayByteBuffer("testConnectAndOptional"
                + "StringSchema");
        pushToXenon(record, map);
        readAndVerify("testConnectAndOptionalStringSchema", record.value());
        client.disconnect();
    }

    // Schema types

    /**
     * Test pushing record to xenon with schema as boolean.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testBooleanToConnect() throws IOException {
        XenonClient client = checkExist("testBooleanToConnect");
        byte[] inputArrTrue = ("{ \"schema\": { \"type\": \"boolean\" }, "
                + "\"payload\": true }").getBytes();
        byte[] inputArrFalse = ("{ \"schema\": { \"type\": \"boolean\"}, "
                + "\"payload\": false }").getBytes();
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true),
                converter.toConnectData(TOPIC, inputArrTrue));
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false),
                converter.toConnectData(TOPIC, inputArrFalse));
        SinkRecord recordTrue = createRecord(Schema.BOOLEAN_SCHEMA, true);
        Map<String, String> map = buildBoolean("testBooleanToConnect");
        pushToXenon(recordTrue, map);
        readAndVerify("testBooleanToConnect", recordTrue.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int8.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testByteToConnect() throws IOException {
        XenonClient client = checkExist("testByteToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int8\" }, "
                + "\"payload\": 7 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 7),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.INT8_SCHEMA, (byte) 7);
        Map<String, String> map = buildByte("testByteToConnect");
        pushToXenon(record, map);
        readAndVerify("testByteToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int16.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testShortToConnect() throws IOException {
        XenonClient client = checkExist("testShortToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int16\" }, "
                + "\"payload\": 10 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 10),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.INT16_SCHEMA, (short) 10);
        Map<String, String> map = buildShort("testShortToConnect");
        pushToXenon(record, map);
        readAndVerify("testShortToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int32.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testIntToConnect() throws IOException {
        XenonClient client = checkExist("testIntToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"int32\" },"
                + " \"payload\": 70 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 70),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.INT32_SCHEMA, 70);
        Map<String, String> map = buildInt("testIntToConnect");
        pushToXenon(record, map);
        readAndVerify("testIntToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as int64.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testLongToConnect() throws IOException {
        XenonClient client = checkExist("testLongToConnect");
        byte[] inputArrLong1 = ("{ \"schema\": { \"type\": \"int64\" },"
                + " \"payload\": 3 }").getBytes();
        byte[] inputArrLong2 = ("{ \"schema\": { \"type\": \"int64\" }, "
                + "\"payload\": 311104111444 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
                converter.toConnectData(TOPIC, inputArrLong1));
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 311104111444L),
                converter.toConnectData(TOPIC, inputArrLong2));
        SinkRecord recordLong1 = createRecord(Schema.INT64_SCHEMA, 3L);
        Map<String, String> map = buildLong("testLongToConnect");
        pushToXenon(recordLong1, map);
        readAndVerify("testLongToConnect", recordLong1.value());
        client.disconnect();
        client = checkExist("testLongToConnect");
        SinkRecord recordLong2 = createRecord(Schema.INT64_SCHEMA, 311104111444L);
        pushToXenon(recordLong2, map);
        readAndVerify("testLongToConnect", recordLong2.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as float32(float).
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testFloatToConnect() throws IOException {
        XenonClient client = checkExist("testFloatToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"float\" }, "
                + "\"payload\": 12.55 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.55f),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.FLOAT32_SCHEMA, 12.55f);
        Map<String, String> map = buildFloat("testFloatToConnect");
        pushToXenon(record, map);
        readAndVerify("testFloatToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as float64(double).
     * Further toConnectData is validated for this case.
     */


    @Test
    public void testDoubleToConnect() throws IOException {
        XenonClient client = checkExist("testDoubleToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"double\" }, "
                + "\"payload\": 23.34 }").getBytes();
        assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 23.34),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.FLOAT64_SCHEMA, 23.34);
        Map<String, String> map = buildDouble("testDoubleToConnect");
        pushToXenon(record, map);
        readAndVerify("testDoubleToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as bytes.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testBytesToConnect() throws IOException {
        ByteBuffer actual = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\" }, "
                + "\"payload\": \"dGVzdC1zdHJpbmc=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        ByteBuffer converted = ByteBuffer.wrap((byte[]) schemaAndValue.value());
        assertEquals(actual, converted);
        SinkRecord recordByteBuffer = createRecord(Schema.BYTES_SCHEMA, actual);
        SinkRecord recordByteArray = createRecord(Schema.BYTES_SCHEMA, converted.array());
        Map<String, String> mapArray = buildByteArrayByteBuffer("testBytesToConnect");
        Map<String, String> mapBuffer = buildByteArrayByteBuffer("testBytesToConnect");
        XenonClient client = checkExist("testBytesToConnect");
        pushToXenon(recordByteBuffer, mapBuffer);
        readAndVerify("testBytesToConnect", recordByteBuffer.value());
        client.disconnect();
        client = checkExist("testBytesToConnect");
        pushToXenon(recordByteArray, mapArray);
        readAndVerify("testBytesToConnect", recordByteArray.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as string.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testStringToConnect() throws IOException {
        XenonClient client = checkExist("testStringToConnect");
        byte[] inputArr = ("{ \"schema\": { \"type\": \"string\" }, "
                + "\"payload\": \"tee-sample\" }").getBytes();
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "tee-sample"),
                converter.toConnectData(TOPIC, inputArr));
        SinkRecord record = createRecord(Schema.STRING_SCHEMA, "tee-sample");
        Map<String, String> map = buildByteArrayByteBuffer("testStringToConnect");
        pushToXenon(record, map);
        readAndVerify("testStringToConnect", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as array.
     * Further toConnectData is validated for this case.
     */
    @Test
    public void testArrayToConnect() throws IOException {
        XenonClient client = checkExist("testArrayToConnect");
        byte[] arrayJson = ("{ \"schema\": { \"type\": \"array\", "
                + "\"items\": { \"type\" : \"int32\" } }, \"payload\": [7, 12, 20] }").getBytes();
        assertEquals(new SchemaAndValue(SchemaBuilder.array(Schema.INT32_SCHEMA)
                        .build(), Arrays.asList(7, 12, 20)),
                converter.toConnectData(TOPIC, arrayJson));
        processArray("testArrayToConnect", Arrays.asList(7, 12, 20));
        readAndVerify("testArrayToConnect", Arrays.asList(7, 12, 20));
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing int32 values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectIntegerValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectIntegerValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"int32\" } }, "
                + "\"payload\": { \"key1\": 3, \"key2\": 4} }").getBytes();
        Map<String, Integer> input = new HashMap<>();
        input.put("key1", 3);
        input.put("key2", 4);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT32_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectIntegerValues", input);
        readAndVerify("testMapToConnectIntegerValues", input);

        client.disconnect();
        XenonClient clientCaseB = checkExist("testMapToConnectIntegerValuesCaseB");
        Map<String, Integer> integerMap = createIntMap(5);
        processMap("testMapToConnectIntegerValuesCaseB", integerMap);
        readAndVerify("testMapToConnectIntegerValuesCaseB", integerMap);
        integerMap.clear();
        clientCaseB.disconnect();
    }


    /**
     * Test pushing record to xenon with schema as map containing boolean values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectBooleanValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectBooleanValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"boolean\" } }, "
                + "\"payload\": { \"key1\": true, \"key2\": false}}").getBytes();
        Map<String, Boolean> input = new HashMap<>();
        input.put("key1", true);
        input.put("key2", false);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.BOOLEAN_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectBooleanValues", input);
        readAndVerify("testMapToConnectBooleanValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing int8 values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectByteValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectByteValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"int8\" } }, "
                + "\"payload\": { \"key1\": 20, \"key2\": 30}}").getBytes();
        Map<String, Byte> input = new HashMap<>();
        input.put("key1", (byte) 20);
        input.put("key2", (byte) 30);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT8_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectByteValues", input);
        readAndVerify("testMapToConnectByteValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing int16 values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectShortValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectShortValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"int16\" } }, "
                + "\"payload\": { \"key1\": 17, \"key2\": 12}}").getBytes();
        Map<String, Short> input = new HashMap<>();
        input.put("key1", (short) 17);
        input.put("key2", (short) 12);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT16_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectShortValues", input);
        readAndVerify("testMapToConnectShortValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing int64 values.
     * Further toConnectData is validated for this case.
     */
    @Test
    public void testMapToConnectLongValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectLongValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"int64\" } }, "
                + "\"payload\": { \"key1\": 20, \"key2\": 4777888999555}}").getBytes();
        Map<String, Long> input = new HashMap<>();
        input.put("key1", 20L);
        input.put("key2", 4777888999555L);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT64_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectLongValues", input);
        readAndVerify("testMapToConnectLongValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing float32 values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectFloatValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectFloatValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"float\" } }, "
                + "\"payload\": { \"key1\": 9.87, \"key2\": 12.98}}").getBytes();
        Map<String, Float> input = new HashMap<>();
        input.put("key1", 9.87f);
        input.put("key2", 12.98f);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.FLOAT32_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectFloatValues", input);
        readAndVerify("testMapToConnectFloatValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing float64 values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectDoubleValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectDoubleValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"double\" } }, "
                + "\"payload\": { \"key1\": 45.76, \"key2\": 67.98}}").getBytes();
        Map<String, Double> input = new HashMap<>();
        input.put("key1", 45.76);
        input.put("key2", 67.98);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.FLOAT64_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        processMap("testMapToConnectDoubleValues", input);
        readAndVerify("testMapToConnectDoubleValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map containing string values.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testMapToConnectStringValues() throws IOException {
        XenonClient client = checkExist("testMapToConnectStringValues");
        byte[] mapJson = ("{ \"schema\": { \"type\": \"map\", "
                + "\"keys\": { \"type\" : \"string\" }, "
                + "\"values\": { \"type\" : \"string\" } }, "
                + "\"payload\": { \"key1\": \"val2\", \"key2\": \"val3\"}}").getBytes();
        Map<String, String> input = new HashMap<>();
        input.put("key1", "val2");
        input.put("key2", "val3");
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.STRING_SCHEMA).build(), input), converter.toConnectData(TOPIC, mapJson));
        SinkRecord record = createRecord(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.STRING_SCHEMA).build(), input);
        processMap("testMapToConnectStringValues", input);
        readAndVerify("testMapToConnectStringValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as struct.
     * The expectedSchema is varied in the cases below .
     * Test whether we can push to xenon and read correctly
     * from it.
     */

    @Test
    public void testStructToConnectFirstCase() throws IOException {
        XenonClient client = checkExist("testStructToConnectFirstCase");
        Schema expectedSchema = SchemaBuilder.struct().version(1)
                .field("Date", Schema.INT32_SCHEMA)
                .field("Type", Schema.STRING_SCHEMA)
                .field("SymbolID", Schema.INT64_SCHEMA)
                .field("SequenceID", Schema.STRING_SCHEMA)
                .field("BuySell", Schema.BOOLEAN_SCHEMA)
                .field("Volume", Schema.INT32_SCHEMA)
                .field("Symbol", Schema.STRING_SCHEMA)
                .field("Durationms", Schema.STRING_SCHEMA)
                .field("Attribute", Schema.STRING_SCHEMA).build();

        Integer date = 20167890;
        String type ;
        Long symID = 32000000L;
        Boolean bs;
        Integer vol = 100;
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            if (i % 2 == 0) {
                bs = true;
                type = "F";
            } else {
                bs = false;
                type = "S";
            }

            Struct expected = new Struct(expectedSchema)
                    .put("Date", date)
                    .put("Type", type)
                    .put("SymbolID", symID)
                    .put("SequenceID", "1211")
                    .put("BuySell", bs)
                    .put("Volume", vol)
                    .put("Symbol", "RR")
                    .put("Durationms", "49890")
                    .put("Attribute", "ABCD");

            date++;
            symID++;
            vol++;
            sinkRecords.add(createRecord(expectedSchema, expected));
        }

        Map<String, String> map = buildStructToConnectFirstCase("testStructToConnectFirstCase");
        pushToXenon(sinkRecords, map);
        XenonSinkConnector connector = new XenonSinkConnector();
        Map<String, Object> checkConfig = connector.config().parse(map);
        readAndVerify("testStructToConnectFirstCase", sinkRecords, (Integer)checkConfig
                .get(XENON_BUFFER_CAPACITY));
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as struct.
     * The expectedSchema is varied in the cases below .
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testStructToConnectSecondCase() throws IOException {
        XenonClient client = checkExist("testStructToConnectSecondCase");
        byte[] structJson = ("{ \"schema\": { \"type\": \"struct\", "
                + "\"optional\": false, "
                + "\"version\": 1,"
                + "\"fields\": "
                + "[{ \"field\": \"field1\", \"type\": \"boolean\" },"
                + " { \"field\": \"field2\", \"type\": \"int8\" }, "
                + "{ \"field\": \"field3\", \"type\": \"int16\"}, "
                + "{ \"field\": \"field4\", \"type\": \"int32\"}, "
                + "{ \"field\": \"field5\", \"type\": \"int64\"}, "
                + "{ \"field\": \"field6\", \"type\": \"float\"}, "
                + "{ \"field\": \"field7\", \"type\": \"double\"}, "
                + "{ \"field\": \"field8\", \"type\": \"boolean\"}, "
                + "{\"field\": \"field9\", \"type\": \"string\"}] }, "
                + "\"payload\": { \"field1\": true, "
                + "\"field2\": 20, "
                + "\"field3\": 70, "
                + "\"field4\": 44, "
                + "\"field5\": 40000000000, "
                + "\"field6\":12.56, "
                + "\"field7\":32.98, "
                + "\"field8\":false, "
                + "\"field9\": \"hello\" } }").getBytes();

        Schema expectedSchema = SchemaBuilder.struct().version(1)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.INT8_SCHEMA)
                .field("field3", Schema.INT16_SCHEMA)
                .field("field4", Schema.INT32_SCHEMA)
                .field("field5", Schema.INT64_SCHEMA)
                .field("field6", Schema.FLOAT32_SCHEMA)
                .field("field7", Schema.FLOAT64_SCHEMA)
                .field("field8", Schema.BOOLEAN_SCHEMA)
                .field("field9", Schema.STRING_SCHEMA).build();

        Struct expected = new Struct(expectedSchema)
                .put("field1", true)
                .put("field2", (byte) 20)
                .put("field3", (short) 70)
                .put("field4", 44)
                .put("field5", 40000000000L)
                .put("field6", 12.56f)
                .put("field7", 32.98)
                .put("field8", false)
                .put("field9", "hello");

        SchemaAndValue converted = converter.toConnectData(TOPIC, structJson);
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
        SinkRecord record = createRecord(expectedSchema, expected);
        Map<String, String> map = buildStructToConnectSecondCase("testStructToConnectSecondCase");
        pushToXenon(record, map);
        readAndVerify("testStructToConnectSecondCase", record.value());
        client.disconnect();
    }


    /**
     * Test pushing record to xenon with schema as null.
     *
     */

    @Test
    public void testNullSchemaAndPrimitivesToConnect() throws IOException {
        //Boolean
        XenonClient client = checkExist("testNullSchemaAndBooleanToConnect");
        byte[] inputArr = "{ \"schema\": null, \"payload\": true }".getBytes();
        SchemaAndValue converted = converter.toConnectData(TOPIC, inputArr);
        assertEquals(new SchemaAndValue(null, true), converted);
        SinkRecord recordTrue = createRecord(null, true);
        Map<String, String> mapTrue = buildBoolean("testNullSchemaAndBooleanToConnect");
        pushToXenon(recordTrue, mapTrue);
        readAndVerify("testNullSchemaAndBooleanToConnect", recordTrue.value());
        client.disconnect();

        //Array
        client = checkExist("testNullSchemaAndArrayToConnect");
        converted = converter.toConnectData(TOPIC, ("{ \"schema\": null, "
                + "\"payload\": [11, \"true\", 80] }").getBytes());
        assertEquals(new SchemaAndValue(null, Arrays.asList(11L, "true", 80L)), converted);
        processArray("testNullSchemaAndArrayToConnect", Arrays.asList(11L, "true", 80L));
        readAndVerify("testNullSchemaAndArrayToConnect", Arrays.asList(11L, "true", 80L));
        client.disconnect();

        //Map
        client = checkExist("testNullSchemaAndMapToConnect");
        converted = converter.toConnectData(TOPIC, ("{ \"schema\": null, "
                + "\"payload\": { \"item1\": 100, \"item2\": 500} }").getBytes());
        Map<String, Long> nullMap = new HashMap<>();
        nullMap.put("item1", 100L);
        nullMap.put("item2", 500L);
        assertEquals(new SchemaAndValue(null, nullMap), converted);
        processMap("testNullSchemaAndMapToConnect", nullMap);
        readAndVerify("testNullSchemaAndMapToConnect", nullMap);
        client.disconnect();
    }


    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     */

    @Test
    public void testDecimalToConnect() throws IOException {
        XenonClient client = checkExist("testDecimalToConnect");
        Schema schema = Decimal.schema(4);
        BigDecimal actual = new BigDecimal(new BigInteger("156"), 4);
        // Payload is base64 encoded byte[]{0, -100}, which is the two's complement encoding of 156.
        String msg = "{ \"schema\": { \"type\": \"bytes\", "
                + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }, \"payload\": \"AJw=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        BigDecimal converted = (BigDecimal) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(actual, converted);
        SinkRecord record = createRecord(schemaAndValue.schema(), converted);
        Map<String, String> map = buildDecimalCaseA("testDecimalToConnect");
        pushToXenon(record, map);
        readAndVerify("testDecimalToConnect", record.value());
        client.disconnect();
    }

    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Date to xenon.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testDateToConnect() throws IOException {
        XenonClient client = checkExist("testDateToConnect");
        Schema schema = Date.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 30000);
        java.util.Date actual = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", "
                + "\"name\": \"org.apache.kafka.connect.data.Date\", "
                + "\"version\": 1 }, \"payload\": 30000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(actual, converted);
        SinkRecord record = createRecord(schemaAndValue.schema(), converted);
        Map<String, String> map = buildLong("testDateToConnect");
        pushToXenon(record, map);
        readAndVerify("testDateToConnect", record.value());
        client.disconnect();
    }



    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Timestamp to xenon.
     * Further toConnectData is validated for this case.
     */

    @Test
    public void testTimestampToConnect() throws IOException {
        XenonClient client = checkExist("testTimestampToConnect");
        Schema schema = Timestamp.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 1000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date actual = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int64\", "
                + "\"name\": \"org.apache.kafka.connect.data.Timestamp\", "
                + "\"version\": 1 }, \"payload\": 3000000000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(actual, converted);
        SinkRecord record = createRecord(schemaAndValue.schema(), converted);
        Map<String, String> map = buildLong("testTimestampToConnect");
        pushToXenon(record, map);
        readAndVerify("testTimestampToConnect", record.value());
        client.disconnect();
    }



    /**
     * Test pushing record to xenon with schema as boolean.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testBooleanToJson() throws IOException {
        XenonClient client = checkExist("testBooleanToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC,
                Schema.BOOLEAN_SCHEMA, true));
        assertEquals(objectMapper.readTree("{ \"type\": \"boolean\", \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals(true, jsonNode.get(PAYLOAD_NAME).booleanValue());
        SinkRecord record = createRecord(Schema.BOOLEAN_SCHEMA, true);
        Map<String, String> map = buildBoolean("testBooleanToJson");
        pushToXenon(record, map);
        readAndVerify("testBooleanToJson", record.value());
        client.disconnect();
    }



    /**
     * Test pushing record to xenon with schema as int64.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testLongToJson() throws IOException {
        XenonClient client = checkExist("testLongToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Schema.INT64_SCHEMA,
                4000000000000L));
        assertEquals(objectMapper.readTree("{ \"type\": \"int64\", \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals(4000000000000L, jsonNode.get(PAYLOAD_NAME)
                .longValue());
        SinkRecord record = createRecord(Schema.INT64_SCHEMA, 4000000000000L);
        Map<String, String> map = buildLong("testLongToJson");
        pushToXenon(record, map);
        readAndVerify("testLongToJson", record.value());
        client.disconnect();
    }


    /**
     * Test pushing record to xenon with schema as float64.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDoubleToJson() throws IOException {
        XenonClient client = checkExist("testDoubleToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Schema.FLOAT64_SCHEMA,
                4.47));
        assertEquals(objectMapper.readTree("{ \"type\": \"double\", \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals(4.47, jsonNode.get(PAYLOAD_NAME).doubleValue(),
                0.001);
        SinkRecord record = createRecord(Schema.FLOAT64_SCHEMA, 4.47);
        Map<String, String> map = buildDouble("testDoubleToJson");
        pushToXenon(record, map);
        readAndVerify("testDoubleToJson", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as bytes.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testBytesToJson() throws IOException {
        XenonClient client = checkExist("testBytesToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA,
                "cost-none".getBytes()));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals(ByteBuffer.wrap("cost-none".getBytes()),
                ByteBuffer.wrap(jsonNode.get(PAYLOAD_NAME).binaryValue()));
        SinkRecord record = createRecord(Schema.BYTES_SCHEMA, "cost-none".getBytes());
        Map<String, String> map = buildByteArrayByteBuffer("testBytesToJson");
        pushToXenon(record, map);
        readAndVerify("testBytesToJson", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as string.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testStringToJson() throws IOException {
        XenonClient client = checkExist("testStringToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA,
                "ringBell"));
        assertEquals(objectMapper.readTree("{ \"type\": \"string\", \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals("ringBell", jsonNode.get(PAYLOAD_NAME).textValue());
        SinkRecord record = createRecord(Schema.STRING_SCHEMA, "ringBell");
        Map<String, String> map = buildByteArrayByteBuffer("testStringToJson");
        pushToXenon(record, map);
        readAndVerify("testStringToJson", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as array.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testArrayToJson() throws IOException {
        XenonClient client = checkExist("testArrayToJson");
        Schema int32Array = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, int32Array,
                Arrays.asList(13, 12, 31)));
        assertEquals(objectMapper.readTree("{ \"type\": \"array\", \"items\": "
                        + "{ \"type\": \"int32\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        assertEquals(JsonNodeFactory.instance.arrayNode().add(13).add(12).add(31),
                jsonNode.get(PAYLOAD_NAME));
        processArray("testArrayToJson", Arrays.asList(13, 12, 31));
        readAndVerify("testArrayToJson", Arrays.asList(13, 12, 31));
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * boolean. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonBooleanValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonBooleanValues");
        Schema stringBoolMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.BOOLEAN_SCHEMA).build();
        Map<String, Boolean> input = new HashMap<>();
        input.put("key1", true);
        input.put("key2", false);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringBoolMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false },"
                        + " \"values\": { \"type\" : \"boolean\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", true)
                .put("key2", false).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonBooleanValues", input);
        readAndVerify("testMapToJsonBooleanValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * int8. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonByteValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonByteValues");
        Schema stringByteMap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
        Map<String, Byte> input = new HashMap<>();
        input.put("key1", (byte) 42);
        input.put("key2", (byte) 35);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringByteMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"int8\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", (byte) 42)
                .put("key2", (byte) 35).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonByteValues", input);
        readAndVerify("testMapToJsonByteValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * int16. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonShortValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonShortValues");
        Schema stringShortMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT16_SCHEMA).build();
        Map<String, Short> input = new HashMap<>();
        input.put("key1", (short) 20);
        input.put("key2", (short) 80);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringShortMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"int16\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", (short) 20)
                .put("key2", (short) 80).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonShortValues", input);
        readAndVerify("testMapToJsonShortValues", input);
        client.disconnect();
    }


    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * int64. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonLongValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonLongValues");
        Schema stringIntMap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build();
        Map<String, Long> input = new HashMap<>();
        input.put("key1", 20L);
        input.put("key2", 50000000L);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringIntMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"int64\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", 20L)
                .put("key2", 50000000L).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        SinkRecord record = createRecord(SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.INT64_SCHEMA).build(), input);
        processMap("testMapToJsonLongValues", input);
        readAndVerify("testMapToJsonLongValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * float32. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonFloatValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonFloatValues");
        Schema stringFloatMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.FLOAT32_SCHEMA).build();
        Map<String, Float> input = new HashMap<>();
        input.put("key1", 20.32f);
        input.put("key2", 15.38f);
        input.put("key3", 15.89f);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringFloatMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\","
                        + " \"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"float\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", 20.32f)
                .put("key2", 15.38f)
                .put("key3", 15.89f).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonFloatValues", input);
        readAndVerify("testMapToJsonFloatValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * float64. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonDoubleValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonDoubleValues");
        Schema stringDoubleMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.FLOAT64_SCHEMA).build();
        Map<String, Double> input = new HashMap<>();
        input.put("key1", 20.32);
        input.put("key2", 15.38);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringDoubleMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"double\", \"optional\": false },"
                        + " \"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", 20.32)
                .put("key2", 15.38).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonDoubleValues", input);
        readAndVerify("testMapToJsonDoubleValues",input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * string. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonStringValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonStringValues");
        Schema stringMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.STRING_SCHEMA).build();
        Map<String, String> input = new HashMap<>();
        input.put("key1", "val1");
        input.put("key2", "val2");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", "val1")
                .put("key2", "val2").arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonStringValues", input);
        readAndVerify("testMapToJsonStringValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as map having
     * keys with schema as string and values with schema as
     * bytes. Further fromConnectData is validated for this case.
     */

    @Test
    public void testMapToJsonBytesValues() throws IOException {
        XenonClient client = checkExist("testMapToJsonBytesValues");
        Schema stringBytesMap = SchemaBuilder.map(Schema.STRING_SCHEMA,
                Schema.BYTES_SCHEMA).build();
        Map<String, byte[]> input = new HashMap<>();
        input.put("key1", "val1".getBytes());
        input.put("key2", "val2".getBytes());
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, stringBytesMap, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"map\", "
                        + "\"keys\": { \"type\" : \"string\", \"optional\": false }, "
                        + "\"values\": { \"type\" : \"bytes\", \"optional\": false }, "
                        + "\"optional\": false }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("key1", "val1".getBytes())
                .put("key2", "val2".getBytes()).arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testMapToJsonBytesValues", input);
        readAndVerify("testMapToJsonBytesValues", input);
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as struct.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testStructToJsonCaseA() throws IOException {
        XenonClient client = checkExist("testStructToJsonCaseA");
        Schema schema = SchemaBuilder.struct().version(1)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .field("field4", Schema.BOOLEAN_SCHEMA)
                .build();
        Struct input = new Struct(schema)
                .put("field1", true)
                .put("field2", "inputB")
                .put("field3", "inputC")
                .put("field4", false);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, schema, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"struct\", "
                        + "\"optional\": false, "
                        + "\"version\": 1,"
                        + " \"fields\": "
                        + "[{ \"field\": \"field1\", \"type\": \"boolean\", \"optional\": false }, "
                        + "{ \"field\": \"field2\", \"type\": \"string\", \"optional\": false }, "
                        + "{ \"field\": \"field3\", \"type\": \"string\", \"optional\": false }, "
                        + "{ \"field\": \"field4\", \"type\": \"boolean\", "
                        + "\"optional\": false }] }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("field1", true)
                .put("field2", "inputB")
                .put("field3", "inputC")
                .put("field4", false)
                .arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        SinkRecord record = createRecord(schema, input);
        Map<String, String> map = buildStructToJsonCaseA("testStructToJsonCaseA");
        pushToXenon(record, map);
        readAndVerify("testStructToJsonCaseA", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as struct with
     * different field schema.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testStructToJsonCaseB() throws IOException {
        XenonClient client = checkExist("testStructToJsonCaseB");
        Schema schema = SchemaBuilder.struct().version(1)
                .field("field1", Schema.BYTES_SCHEMA)
                .field("field2", Schema.INT8_SCHEMA)
                .field("field3", Schema.INT16_SCHEMA)
                .field("field4", Schema.INT32_SCHEMA)
                .field("field5", Schema.INT64_SCHEMA)
                .field("field6", Schema.FLOAT32_SCHEMA)
                .field("field7", Schema.FLOAT64_SCHEMA)
                .build();
        Struct input = new Struct(schema)
                .put("field1", "true".getBytes())
                .put("field2", (byte) 17)
                .put("field3", (short) 25)
                .put("field4", 200)
                .put("field5", 400000000L)
                .put("field6", 20.32f)
                .put("field7", 40.34);
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, schema, input));
        assertEquals(objectMapper.readTree("{ \"type\": \"struct\", "
                        + "\"optional\": false, "
                        + "\"version\": 1,"
                        + "\"fields\": "
                        + "[{ \"field\": \"field1\", \"type\": \"bytes\", \"optional\": false }, "
                        + "{ \"field\": \"field2\", \"type\": \"int8\", \"optional\": false }, "
                        + "{ \"field\": \"field3\", \"type\": \"int16\", \"optional\": false }, "
                        + "{ \"field\": \"field4\", \"type\": \"int32\", \"optional\": false }, "
                        + "{ \"field\": \"field5\", \"type\": \"int64\", \"optional\": false }, "
                        + "{ \"field\": \"field6\", \"type\": \"float\", \"optional\": false }, "
                        + "{ \"field\": \"field7\", \"type\": \"double\", "
                        + "\"optional\": false }] }"),
                jsonNode.get(SCHEMA_NAME));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("field1", "true".getBytes())
                .put("field2", (byte) 17)
                .put("field3", (short) 25)
                .put("field4", 200)
                .put("field5", 400000000L)
                .put("field6", 20.32f)
                .put("field7", 40.34)
                .arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        SinkRecord record = createRecord(schema, input);
        Map<String, String> map = buildStructToJsonCaseB("testStructToJsonCaseB");
        pushToXenon(record, map);
        readAndVerify("testStructToJsonCaseB", record.value());
        client.disconnect();
    }

    /**
     * Test to validate correct version number retrieval
     * from created schema.
     */

    @Test
    public void testVersionExtracted() throws IOException {
        Schema oldSchema = SchemaBuilder.struct().version(1)
                .field("latitude", Schema.INT32_SCHEMA)
                .field("longitude", Schema.INT64_SCHEMA)
                .build();

        Struct oldExpected = new Struct(oldSchema)
                .put("latitude", 20)
                .put("longitude", 30L);

        SchemaAndValue oldGeoSchemaAndVal = new SchemaAndValue(oldSchema, oldExpected);
        byte[] oldSerializedRecord = converter.fromConnectData(TOPIC, oldGeoSchemaAndVal.schema(),
                oldGeoSchemaAndVal.value());
        SchemaAndValue oldConverted = converter.toConnectData(TOPIC, oldSerializedRecord);
        assertEquals(1L, (long) oldConverted.schema().version());

        //validating version change with new schema
        Schema newSchema = SchemaBuilder.struct().version(2)
                .field("LatAndLong", Schema.BOOLEAN_SCHEMA)
                .build();
        Struct newExpected = new Struct(newSchema)
                .put("LatAndLong", true);
        SchemaAndValue newGeoSchemaAndVal = new SchemaAndValue(newSchema, newExpected);
        byte[] newSerializedRecord = converter.fromConnectData(TOPIC, newGeoSchemaAndVal.schema(),
                newGeoSchemaAndVal.value());
        SchemaAndValue newConverted = converter.toConnectData(TOPIC, newSerializedRecord);
        assertEquals(2L, (long) newConverted.schema().version());
    }


    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDecimalToJsonCaseA() throws IOException {
        XenonClient client = checkExist("testDecimalToJsonCaseA");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Decimal.schema(4),
                new BigDecimal(new BigInteger("156"), 4)));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                        + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }"),
                jsonNode.get(SCHEMA_NAME));
        assertArrayEquals(new byte[]{0, -100},
                jsonNode.get(PAYLOAD_NAME).binaryValue());
        SinkRecord record = createRecord(Decimal.schema(4),
                new BigDecimal(new BigInteger("156788888"), 4));
        Map<String, String> map = buildDecimalCaseA("testDecimalToJsonCaseA");
        pushToXenon(record, map);
        readAndVerify("testDecimalToJsonCaseA", record.value());
        client.disconnect();
    }


    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDecimalToJsonCaseB() throws IOException {
        XenonClient client = checkExist("testDecimalToJsonCaseB");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Decimal.schema(4),
                new BigDecimal(new BigInteger("156"), 4)));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                        + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }"),
                jsonNode.get(SCHEMA_NAME));
        assertArrayEquals(new byte[]{0, -100},
                jsonNode.get(PAYLOAD_NAME).binaryValue());
        SinkRecord record = createRecord(Decimal.schema(6),
                new BigDecimal(new BigInteger("156788888822221166"), 6));
        Map<String, String> map = buildDecimalCaseB("testDecimalToJsonCaseB");
        pushToXenon(record, map);
        readAndVerify("testDecimalToJsonCaseB", record.value());
        client.disconnect();
    }


    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDecimalToJsonCaseC() throws IOException {
        XenonClient client = checkExist("testDecimalToJsonCaseC");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Decimal.schema(4),
                new BigDecimal(new BigInteger("156"), 4)));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                        + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }"),
                jsonNode.get(SCHEMA_NAME));
        assertArrayEquals(new byte[]{0, -100},
                jsonNode.get(PAYLOAD_NAME).binaryValue());
        SinkRecord record = createRecord(Decimal.schema(6),
                new BigDecimal(new BigInteger("156788888822221166788799555"), 6));
        Map<String, String> map = buildDecimalCaseC("testDecimalToJsonCaseC");
        pushToXenon(record, map);
        readAndVerify("testDecimalToJsonCaseC", record.value());
        client.disconnect();
    }


    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDecimalToJsonCaseD() throws IOException {
        XenonClient client = checkExist("testDecimalToJsonCaseD");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Decimal.schema(4),
                new BigDecimal(new BigInteger("156"), 4)));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                        + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }"),
                jsonNode.get(SCHEMA_NAME));
        assertArrayEquals(new byte[]{0, -100},
                jsonNode.get(PAYLOAD_NAME).binaryValue());
        SinkRecord record = createRecord(Decimal.schema(6),
                new BigDecimal(new BigInteger("156788888822221166788799555555444220"), 6));
        Map<String, String> map = buildDecimalCaseD("testDecimalToJsonCaseD");
        pushToXenon(record, map);
        readAndVerify("testDecimalToJsonCaseD", record.value());
        client.disconnect();
    }

    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Decimal to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testDecimalToJsonCaseE() throws IOException {
        XenonClient client = checkExist("testDecimalToJsonCaseE");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Decimal.schema(4),
                new BigDecimal(new BigInteger("156"), 4)));
        assertEquals(objectMapper.readTree("{ \"type\": \"bytes\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Decimal\", "
                        + "\"version\": 1, \"parameters\": { \"scale\": \"4\" } }"),
                jsonNode.get(SCHEMA_NAME));
        assertArrayEquals(new byte[]{0, -100},
                jsonNode.get(PAYLOAD_NAME).binaryValue());
        SinkRecord record = createRecord(Decimal.schema(6),
                new BigDecimal(new BigInteger("1567888888222211667887995555554442"), 6));
        Map<String, String> map = buildDecimalCaseE("testDecimalToJsonCaseE");
        pushToXenon(record, map);
        readAndVerify("testDecimalToJsonCaseE", record.value());
        client.disconnect();
    }



    /**
     * Handling logical types. The check is done using schema.name().
     * The case below is for pushing Time to xenon.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testTimeToJson() throws IOException {
        XenonClient client = checkExist("testTimeToJson");
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 10000000);
        java.util.Date date = calendar.getTime();
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, Time.SCHEMA, date));
        assertEquals(objectMapper.readTree("{ \"type\": \"int32\", \"optional\": false, "
                        + "\"name\": \"org.apache.kafka.connect.data.Time\", "
                        + "\"version\": 1 }"),
                jsonNode.get(SCHEMA_NAME));
        JsonNode payload = jsonNode.get(PAYLOAD_NAME);
        assertTrue(payload.isInt());
        assertEquals(10000000, payload.longValue());
        SinkRecord record = createRecord(Time.SCHEMA, date);
        Map<String, String> map = buildLong("testTimeToJson");
        pushToXenon(record, map);
        readAndVerify("testTimeToJson", record.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as null.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testNullSchemaAndPrimitivesToJson() throws IOException {
        JsonNode jsonNodeBool = objectMapper.readTree(converter.fromConnectData(TOPIC, null, true));
        JsonNode jsonNodeString = objectMapper.readTree(converter.fromConnectData(TOPIC, null,
                "hey-there"));
        JsonNode jsonNodeByte = objectMapper.readTree(converter.fromConnectData(TOPIC, null, (byte) 5));
        JsonNode jsonNodeShort = objectMapper.readTree(converter.fromConnectData(TOPIC, null, (short) 5));
        JsonNode jsonNodeInt = objectMapper.readTree(converter.fromConnectData(TOPIC, null, 5));
        JsonNode jsonNodeLong = objectMapper.readTree(converter.fromConnectData(TOPIC, null, (long) 2000));
        JsonNode jsonNodeFloat = objectMapper.readTree(converter.fromConnectData(TOPIC, null, 5.34f));
        JsonNode jsonNodeDouble = objectMapper.readTree(converter.fromConnectData(TOPIC, null, 5.34));

        assertEquals(true, jsonNodeBool.get(PAYLOAD_NAME).booleanValue());
        assertEquals("hey-there", jsonNodeString.get(PAYLOAD_NAME).textValue());
        assertEquals(5, jsonNodeByte.get(PAYLOAD_NAME).intValue());
        assertEquals(5, jsonNodeShort.get(PAYLOAD_NAME).intValue());
        assertEquals(5, jsonNodeInt.get(PAYLOAD_NAME).intValue());
        assertEquals(2000, jsonNodeLong.get(PAYLOAD_NAME).intValue());
        assertEquals(5.34f, jsonNodeFloat.get(PAYLOAD_NAME).floatValue(), 0.001);
        assertEquals(5.34, jsonNodeDouble.get(PAYLOAD_NAME).doubleValue(), 0.001);

        SinkRecord recordBool = createRecord(null,
                jsonNodeBool.get(PAYLOAD_NAME).booleanValue());
        SinkRecord recordString = createRecord(null,
                jsonNodeString.get(PAYLOAD_NAME).textValue());
        SinkRecord recordByte = createRecord(null,
                (byte) jsonNodeByte.get(PAYLOAD_NAME).intValue());
        SinkRecord recordShort = createRecord(null,
                (short) jsonNodeShort.get(PAYLOAD_NAME).intValue());
        SinkRecord recordInt = createRecord(null,
                jsonNodeInt.get(PAYLOAD_NAME).intValue());
        SinkRecord recordLong = createRecord(null,
                (long) jsonNodeLong.get(PAYLOAD_NAME).intValue());
        SinkRecord recordFloat = createRecord(null,
                jsonNodeFloat.get(PAYLOAD_NAME).floatValue());
        SinkRecord recordDouble = createRecord(null,
                jsonNodeDouble.get(PAYLOAD_NAME).doubleValue());
        //BOOL
        XenonClient client = checkExist("testNullSchemaAndBooleanToJson");
        Map<String, String> mapBool = buildBoolean("testNullSchemaAndBooleanToJson");
        pushToXenon(recordBool, mapBool);
        readAndVerify("testNullSchemaAndBooleanToJson", recordBool.value());
        client.disconnect();

        //STRING
        client = checkExist("testNullSchemaAndStringToJson");
        Map<String, String> mapString = buildByteArrayByteBuffer("testNullSchemaAndStringToJson");
        pushToXenon(recordString, mapString);
        readAndVerify("testNullSchemaAndStringToJson", recordString.value());
        client.disconnect();

        //BYTE
        client = checkExist("testNullSchemaAndByteToJson");
        Map<String, String> mapByte = buildByte("testNullSchemaAndByteToJson");
        pushToXenon(recordByte, mapByte);
        readAndVerify("testNullSchemaAndByteToJson", recordByte.value());
        client.disconnect();

        //SHORT
        client = checkExist("testNullSchemaAndShortToJson");
        Map<String, String> mapShort = buildShort("testNullSchemaAndShortToJson");
        pushToXenon(recordShort, mapShort);
        readAndVerify("testNullSchemaAndShortToJson", recordShort.value());
        client.disconnect();

        //INT
        client = checkExist("testNullSchemaAndIntToJson");
        Map<String, String> mapInt = buildInt("testNullSchemaAndIntToJson");
        pushToXenon(recordInt, mapInt);
        readAndVerify("testNullSchemaAndIntToJson", recordInt.value());
        client.disconnect();

        //LONG
        client = checkExist("testNullSchemaAndLongToJson");
        Map<String, String> mapLong = buildLong("testNullSchemaAndLongToJson");
        pushToXenon(recordLong, mapLong);
        readAndVerify("testNullSchemaAndLongToJson", recordLong.value());
        client.disconnect();

        //FLOAT
        client = checkExist("testNullSchemaAndFloatToJson");
        Map<String, String> mapFloat = buildFloat("testNullSchemaAndFloatToJson");
        pushToXenon(recordFloat, mapFloat);
        readAndVerify("testNullSchemaAndFloatToJson", recordFloat.value());
        client.disconnect();

        //DOUBLE
        client = checkExist("testNullSchemaAndDoubleToJson");
        Map<String, String> mapDouble = buildDouble("testNullSchemaAndDoubleToJson");
        pushToXenon(recordDouble, mapDouble);
        readAndVerify("testNullSchemaAndDoubleToJson", recordDouble.value());
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as null.
     * Here the array is passed with null schema implying that
     * any schema values may be passed as its elements("anything goes").
     * For instance here record value is an array with elements of
     * int32,string and boolean schema.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testNullSchemaAndArrayToJson() throws IOException {
        XenonClient client = checkExist("testNullSchemaAndArrayToJson");
        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, null,
                Arrays.asList(19, "string-check", true)));
        assertEquals(JsonNodeFactory.instance.arrayNode()
                        .add(19)
                        .add("string-check")
                        .add(true),
                jsonNode.get(PAYLOAD_NAME));
        processArray("testNullSchemaAndArrayToJson", Arrays.asList(19, "string-check", true));
        readAndVerify("testNullSchemaAndArrayToJson", Arrays.asList(19, "string-check", true));
        client.disconnect();
    }

    /**
     * Test pushing record to xenon with schema as null.
     * Here the map is passed with null schema implying that
     * any schema values may be passed as its elements. For instance
     * here record value is a map with elements of
     * int64,string and boolean schema.
     * Further fromConnectData is validated for this case.
     */

    @Test
    public void testNullSchemaAndMapToJson() throws IOException {
        XenonClient client = checkExist("testNullSchemaAndMapToJson");
        Map<String, Object> input = new HashMap<>();
        input.put("inp1", 40000000L);
        input.put("inp2", "old");
        input.put("inp3", true);

        JsonNode jsonNode = objectMapper.readTree(converter.fromConnectData(TOPIC, null, input));
        ArrayNode actual = JsonNodeFactory.instance.objectNode()
                .put("inp1", 40000000L)
                .put("inp2", "old")
                .put("inp3", true)
                .arrayNode();
        ArrayNode expected = ((ObjectNode) jsonNode.get(PAYLOAD_NAME)).arrayNode();
        assertEquals(actual, expected);
        processMap("testNullSchemaAndMapToJson", input);
        readAndVerify("testNullSchemaAndMapToJson", input);
        client.disconnect();
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Boolean and payload null.
     */
    private Map<String, String> buildNullBoolean(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:BOOL}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Byte/int8 and payload null.
     */
    private Map<String, String> buildNullByte(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:I8}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Short/int16 and payload null.
     */
    private Map<String, String> buildNullShort(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:I16}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Int/int32 and payload null.
     */
    private Map<String, String> buildNullInt(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:I32}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Long/int64 and payload null.
     */
    private Map<String, String> buildNullLong(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:I64}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Float/float32 and payload null.
     */
    private Map<String, String> buildNullFloat(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:FLOAT}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Double/float64 and payload null.
     */
    private Map<String, String> buildNullDouble(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:DOUBLE}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as byte[], ByteBuffer or String
     * and payload null.
     */
    private Map<String, String> buildNullByteArrayByteBuffer(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{fieldNull:CHAR}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Boolean.
     */
    private Map<String, String> buildBoolean(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_BUFFER_CAPACITY, "10");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{BuySell:BOOL}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Byte/int8.
     */
    private Map<String, String> buildByte(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:I8}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Short/int16.
     */
    private Map<String, String> buildShort(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:I16}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Integer/int32.
     */
    private Map<String, String> buildInt(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{UUID:I32}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Long/int64.
     */
    private Map<String, String> buildLong(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:I64}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Float/float32.
     */
    private Map<String, String> buildFloat(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:FLOAT}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Double/float64.
     */
    private Map<String, String> buildDouble(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{STOCKS:DOUBLE}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as byte[], ByteBuffer or String.
     */
    private Map<String, String> buildByteArrayByteBuffer(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SeqID:CHAR}");
        return sampleConfig;

    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Struct with 9 fields.
     */
    private Map<String, String> buildStructToConnectFirstCase(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_BUFFER_CAPACITY,"100");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:I32, Type:CHAR, "
                + "SymbolID:I64, SequenceID:CHAR, BuySell:BOOL, Volume:I32, "
                + "Symbol:CHAR, Durationms:CHAR, Attribute:CHAR}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Struct with 9 fields different
     * than above.
     */
    private Map<String, String> buildStructToConnectSecondCase(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{f1:BOOL, f2:I8, f3:I16, f4:I32,"
                + " f5:I64, f6:FLOAT, f7:DOUBLE, f8:BOOL, f9:CHAR}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Struct with 4 fields with
     * different schemas.
     */
    private Map<String, String> buildStructToJsonCaseA(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{f1:BOOL, f2:CHAR, f3:CHAR, f4:BOOL}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Struct with 7 fields with
     * different schemas.
     */
    private Map<String, String> buildStructToJsonCaseB(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{f1:CHAR, f2:I8, f3:I16, f4:I32, f5:I64, "
                + "f6:FLOAT, f7:DOUBLE}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildDecimalCaseA(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:DECIMAL[9,4]}");
        return sampleConfig;
    }

    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildDecimalCaseB(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:DECIMAL[18,6]}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildDecimalCaseC(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:DECIMAL[27,6]}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildDecimalCaseD(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:DECIMAL[36,6]}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildDecimalCaseE(String s) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, "{SymbolID:DECIMAL[36,6]}");
        return sampleConfig;
    }


    /**
     * Function that returns sampleConfig map
     * of XenonConfiguration for pushing values
     * with schema as Decimal.
     */
    private Map<String, String> buildNestedArray(String s, String datasetSchema) {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, TOPIC);
        sampleConfig.put(XENON_DATASET_NAME, s);
        sampleConfig.put(XENON_DATASET_SCHEMA, datasetSchema);
        return sampleConfig;
    }


    /**
     * Function to uniquely process each value inside Array
     * based on its instance and then using the appropriate
     * configuration map, push the record value into Xenon.
     *
     * @param x - record.value() passed as an object.
     */
    private void processArray(String s, Object x) {
        Collection collection = (Collection) x;
        //get datasetSchema using the array to be opened in xenon.
        String datasetSchema = schemaCreationArray(collection);
        String[] fields = datasetSchema
                .substring(1, datasetSchema.length() - 1)
                .split(", ");
        //HashSet to store each field schema value opened in xenon.
        //If the schema value is same for all fields then all records in
        //array have same schema.
        //Else the array is passed with null schema to include records with
        //different schemas.
        Set<String> valueSchema = new HashSet<>();
        for (String field : fields) {
            String fieldType = field.split(":")[1];
            valueSchema.add(fieldType);
        }
        boolean isEqualSchema = false;
        if (valueSchema.size() == 1){
            isEqualSchema = true;
        }
        ArrayList<Object> list = new ArrayList<>();
        for (Object rec : collection) {
            //Storing record schema while parsing from collection.
            if (isEqualSchema){
                if (rec instanceof Byte) {
                    recSchema = SchemaBuilder
                            .array(Schema.INT8_SCHEMA)
                            .build();
                } else if (rec instanceof Short){
                    recSchema = SchemaBuilder
                            .array(Schema.INT16_SCHEMA)
                            .build();
                } else if (rec instanceof Integer){
                    recSchema = SchemaBuilder
                            .array(Schema.INT32_SCHEMA)
                            .build();
                } else if (rec instanceof Long){
                    recSchema = SchemaBuilder
                            .array(Schema.INT64_SCHEMA)
                            .build();
                } else if (rec instanceof Float){
                    recSchema = SchemaBuilder
                            .array(Schema.FLOAT32_SCHEMA)
                            .build();
                } else if (rec instanceof Double){
                    recSchema = SchemaBuilder
                            .array(Schema.FLOAT64_SCHEMA)
                            .build();
                } else if (rec instanceof Boolean) {
                    recSchema = SchemaBuilder
                            .array(Schema.BOOLEAN_SCHEMA)
                            .build();
                } else if(rec instanceof String){
                    recSchema = SchemaBuilder
                            .array(Schema.STRING_SCHEMA)
                            .build();
                } else if (rec instanceof byte[] || rec instanceof ByteBuffer){
                    recSchema = SchemaBuilder
                            .array(Schema.BYTES_SCHEMA)
                            .build();
                } else {
                    throw new ConnectException("record class not supported.");
                }
            }
            if (rec != null && !(rec instanceof Collection)) {
                list.add(rec);
            } else if (rec == null) {
                throw new ConnectException("Invalid null value for required field "
                        + "in Array input");
            } else {
                throw new ConnectException("Unacceptable nested array value");
            }
        }

        if (!isEqualSchema) {
            SinkRecord record = createRecord(null, list);
            Map<String, String> mapRec = buildNestedArray(s, datasetSchema);
            pushToXenon(record, mapRec);
        }else {
            SinkRecord record = createRecord(recSchema, list);
            Map<String, String> mapRec = buildNestedArray(s, datasetSchema);
            pushToXenon(record, mapRec);
        }
        list.clear();
    }


    /**
     * Function to uniquely process each value inside Map
     * based on its instance and then using the appropriate
     * configuration map, push the record value into Xenon.
     *
     * @param x - record.value() passed as an object.
     * @param s - datasetName to be opened in xenon.
     */
    private void processMap(String s, Object x) {
        Map<?,?> map = (Map<?,?>) x;
        //get datasetSchema using the map to be opened in xenon.
        String datasetSchema = schemaCreationMap(map);
        String[] fields = datasetSchema
                .substring(1, datasetSchema.length() - 1)
                .split(", ");
        //HashSet to store each field schema value opened in xenon.
        //If the schema value is same for all fields then all records in
        //array have same schema.
        //Else the array is passed with null schema to include records with
        //different schemas.
        Set<String> valueSchema = new HashSet<>();
        for (String field : fields) {
            String fieldType = field.split(":")[1];
            valueSchema.add(fieldType);
        }
        boolean isEqualSchema = false;
        if (valueSchema.size() == 1){
            isEqualSchema = true;
        }
        ArrayList<Object> list = new ArrayList<>();
        for (Object testRec : map.values()) {
            if (isEqualSchema){
                if (testRec instanceof Byte) {
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.INT8_SCHEMA)
                            .build();
                } else if (testRec instanceof Short){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.INT16_SCHEMA)
                            .build();
                } else if (testRec instanceof Integer){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.INT32_SCHEMA)
                            .build();
                } else if (testRec instanceof Long){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.INT64_SCHEMA)
                            .build();
                } else if (testRec instanceof Float){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.FLOAT32_SCHEMA)
                            .build();
                } else if (testRec instanceof Double){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.FLOAT64_SCHEMA)
                            .build();
                } else if (testRec instanceof Boolean) {
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.BOOLEAN_SCHEMA)
                            .build();
                } else if(testRec instanceof String){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.STRING_SCHEMA)
                            .build();
                } else if (testRec instanceof byte[] || testRec instanceof ByteBuffer){
                    recSchema = SchemaBuilder
                            .map(Schema.STRING_SCHEMA,
                                    Schema.BYTES_SCHEMA)
                            .build();
                } else {
                    throw new ConnectException("record class not supported.");
                }
            }
            if (testRec != null && !(testRec instanceof Map)) {
                list.add(testRec);
            } else if (testRec == null) {
                throw new ConnectException("Invalid null value for required field "
                        + "in Array input");
            } else {
                throw new ConnectException("Unacceptable nested array value");
            }
        }

        if (!isEqualSchema) {
            SinkRecord record = createRecord(null, list);
            Map<String, String> mapRec = buildNestedArray(s, datasetSchema);
            pushToXenon(record, mapRec);
        } else {
            SinkRecord record = createRecord(recSchema, list);
            Map<String, String> mapRec = buildNestedArray(s, datasetSchema);
            pushToXenon(record, mapRec);
        }
        list.clear();
    }

    /**
     * Function responsible to push Kafka record into Xenon.
     *
     * @param record - Kafka record.
     * @param props  -  Configuration map based on record schema.
     */
    private void pushToXenon(SinkRecord record, Map<String, String> props) {
        PowerMock.replayAll();
        task = new XenonSinkTask();
        task.initialize(ctx);
        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        sinkRecords.add(record);
        task.start(props);
        task.put(sinkRecords);
        task.stop();
        PowerMock.verifyAll();
    }


    /**
     * Function responsible to push Kafka record into Xenon.
     *
     * @param sinkRecords - Collection of Kafka records.
     * @param props  -  Configuration map based on record schema.
     */
    private void pushToXenon(Collection<SinkRecord> sinkRecords, Map<String, String> props) {
        PowerMock.replayAll();
        task = new XenonSinkTask();
        task.initialize(ctx);
        task.start(props);
        task.put(sinkRecords);
        task.stop();
        PowerMock.verifyAll();
    }


    /**
     * method to read what was written to xenon.
     *
     * @param datasetName - name of dataset in xenon to read from.
     * @param x - record value.
     */
    private void readAndVerify(String datasetName, Object x) {
        XenonReader reader = new XenonReader("localhost", 41000, 100);
        try {
            Vector<String> results = reader.readFromXenon(datasetName);
            if (x != null) {
                switch (x.getClass().getSimpleName()) {

                    case "Struct":
                        Vector<String> structVal = new Vector<>();
                        for (Field field : ((Struct) x).schema().fields()) {
                            if (!((Struct) x).get(field).getClass().getSimpleName().equals
                                    ("byte[]")) {
                                structVal.add(String.valueOf(((Struct) x).get(field)));
                            } else {
                                //log.info("byte[] case");
                                structVal.add( new String ((byte[])((Struct) x).get(field)));
                            }
                        }
                        String xenonRecord = String.valueOf(results);
                        String inputRecordVal = String.valueOf(structVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Date":
                        Vector<String> logicalVal = new Vector<>();
                        java.util.Date converted = (java.util.Date) x;
                        logicalVal.add(String.valueOf(converted.getTime()));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(logicalVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "byte[]":
                        Vector<String> byteArrVal = new Vector<>();
                        String value = new String((byte[]) x);
                        byteArrVal.add(value);
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(byteArrVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "HeapByteBuffer":
                        Vector<String> byteBufferVal = new Vector<>();
                        value = new String(((ByteBuffer) x).array());
                        byteBufferVal.add(value);
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(byteBufferVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "BigDecimal":
                        Vector<String> decVal = new Vector<>();
                        decVal.add(String.valueOf((BigDecimal)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(decVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "ArrayList":
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(x);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "HashMap":
                        Vector<String> mapVal = new Vector<>();
                        for (Object e : ((Map) x).values()) {
                            if (e.getClass().getSimpleName().equals("byte[]")) {
                                mapVal.add(new String((byte[]) e));
                            } else if (e.getClass().getSimpleName().equals("HeapByteBuffer")) {
                                mapVal.add(new String(((ByteBuffer) e).array()));
                            } else {
                                mapVal.add(String.valueOf(e));
                            }
                        }
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(mapVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Boolean":
                        Vector<String> boolVal = new Vector<>();
                        boolVal.add(String.valueOf((boolean)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(boolVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Long":
                        Vector<String> longVal = new Vector<>();
                        longVal.add(String.valueOf((long)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(longVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Integer":
                        Vector<String> intVal = new Vector<>();
                        intVal.add(String.valueOf((int)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(intVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Short":
                        Vector<String> shortVal = new Vector<>();
                        shortVal.add(String.valueOf((short)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(shortVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Byte":
                        Vector<String> byteVal = new Vector<>();
                        byteVal.add(String.valueOf((byte)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(byteVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "String":
                        Vector<String> stringVal = new Vector<>();
                        stringVal.add(String.valueOf(x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(stringVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    case "Double":
                        Vector<String> dblVal = new Vector<>();
                        dblVal.add(String.valueOf((double)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(dblVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;


                    case "Float":
                        Vector<String> fltVal = new Vector<>();
                        fltVal.add(String.valueOf((float)x));
                        xenonRecord = String.valueOf(results);
                        inputRecordVal = String.valueOf(fltVal);
                        compareResults(xenonRecord, inputRecordVal);
                        break;

                    default:
                        throw new ConnectException("class not matched.");
                }

            } else {
                Vector<String> nullVal = new Vector<>();
                nullVal.add(String.valueOf(x));
                String xenonRecord = String.valueOf(results);
                String inputRecordVal = String.valueOf(nullVal);
                compareResults(xenonRecord, inputRecordVal);
            }

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        PowerMock.verifyAll();
    }


    /**
     * method to read what was written to xenon. This highlights case of multiple records
     * pushed to xenon with Struct schema.
     *
     * @param datasetName - name of dataset in xenon to read from.
     * @param sinkRecords - collection of kafka records.
     */
    private void readAndVerify(String datasetName, Collection<SinkRecord> sinkRecords,
                               Integer bufferSize) {
        XenonReader reader = new XenonReader("localhost", 41000, bufferSize);
        try {
            Vector<String> results = reader.readFromXenon(datasetName);
            Vector<String> structVal = new Vector<>();
            for (SinkRecord record : sinkRecords) {
                if (record != null) {
                    for (Field field : ((Struct) record.value()).schema().fields()) {
                        if (!((Struct) record.value()).get(field).getClass().getSimpleName().equals
                                ("byte[]")) {
                            structVal.add(String.valueOf(((Struct) record.value()).get(field)));
                        } else {
                            //log.info("byte[] case");
                            structVal.add(new String((byte[]) ((Struct) record.value()).get(field)));
                        }
                    }
                } else {
                    throw new ConnectException("null val not expected.");
                }
            }
            String xenonRecord = String.valueOf(results);
            String inputRecordVal = String.valueOf(structVal);
            compareResults(xenonRecord, inputRecordVal);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        PowerMock.verifyAll();
    }



    /**
     * Creating xenon dataset schema using collection of records.
     *
     * @param collection - input record collection.
     * @return schema - datasetSchema as string.
     */
    private String schemaCreationArray(Collection collection) {
        int i = 0;
        StringBuilder datasetSchemaArray = new StringBuilder();
        for (Object rec : collection) {
            if (rec != null && !(rec instanceof Collection)) {
                if (rec instanceof Boolean) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldBool")
                            .append(i)
                            .append(":BOOL");
                } else if (rec instanceof Double) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldDouble")
                            .append(i)
                            .append(":DOUBLE");

                } else if (rec instanceof Float) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldFloat")
                            .append(i)
                            .append(":FLOAT");

                } else if (rec instanceof Long) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldLong")
                            .append(i)
                            .append(":I64");

                } else if (rec instanceof Integer) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldInt")
                            .append(i)
                            .append(":I32");

                } else if (rec instanceof Short) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldShort")
                            .append(i)
                            .append(":I16");

                } else if (rec instanceof Byte) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldByte")
                            .append(i)
                            .append(":I8");

                } else if (rec instanceof byte[] || rec instanceof ByteBuffer ||
                        rec instanceof String) {
                    i += 1;
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldBytes")
                            .append(i)
                            .append(":CHAR");

                } else if (rec instanceof BigDecimal) {
                    i += 1;
                    Integer precision = ((BigDecimal) rec).precision();
                    Integer scale = ((BigDecimal) rec).scale();
                    if (datasetSchemaArray.length() != 0) {
                        datasetSchemaArray.append(", ");
                    }
                    datasetSchemaArray.append("FieldDecimal")
                            .append(i)
                            .append(":DECIMAL")
                            .append("[")
                            .append(precision)
                            .append(",")
                            .append(scale)
                            .append("]");

                }
            } else {
                throw new ConnectException("Unacceptable schema check");
            }
        }
        datasetSchemaArray.insert(0, "{")
                .insert(datasetSchemaArray.length(), "}");
        return datasetSchemaArray.toString();
    }


    /**
     * Creating xenon dataset schema using collection of records.
     *
     * @param map - input record collection as a map.
     * @return schema - datasetSchema as string.
     */
    private String schemaCreationMap(Map map) {
        int i = 0;
        StringBuilder datasetSchemaMap = new StringBuilder();
        for (Object testMap : map.values()) {
            if (testMap != null && !(testMap instanceof Map)) {
                if (testMap instanceof Boolean) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldBool")
                            .append(i)
                            .append(":BOOL");

                } else if (testMap instanceof Double) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldDouble")
                            .append(i)
                            .append(":DOUBLE");
                } else if (testMap instanceof Float) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldFloat")
                            .append(i)
                            .append(":FLOAT");

                } else if (testMap instanceof Long) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldLong")
                            .append(i)
                            .append(":I64");

                } else if (testMap instanceof Integer) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldInt")
                            .append(i)
                            .append(":I32");

                } else if (testMap instanceof Short) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldShort")
                            .append(i)
                            .append(":I16");

                } else if (testMap instanceof Byte) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldByte")
                            .append(i)
                            .append(":I8");

                } else if (testMap instanceof byte[] || testMap instanceof
                        ByteBuffer ||
                        testMap instanceof String) {
                    i += 1;
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldBytes")
                            .append(i)
                            .append(":CHAR");

                } else if (testMap instanceof BigDecimal) {
                    i += 1;
                    Integer precision = ((BigDecimal) testMap).precision();
                    Integer scale = ((BigDecimal) testMap).scale();
                    if (datasetSchemaMap.length() != 0) {
                        datasetSchemaMap.append(", ");
                    }
                    datasetSchemaMap.append("FieldDecimal")
                            .append(i)
                            .append(":DECIMAL")
                            .append("[")
                            .append(precision)
                            .append(",")
                            .append(scale)
                            .append("]");
                }
            } else {
                throw new ConnectException("Unacceptable schema check");
            }
        }

        datasetSchemaMap.insert(0, "{")
                .insert(datasetSchemaMap.length(), "}");
        return datasetSchemaMap.toString();
    }


    /**
     * Function to compare whether the kafka record pushed is same as the one read from xenon.
     *
     * @param xenonRead   - Record read from xenon.
     *                    PowerMock.replayAll();
     * @param kafkaRecord - Input kafka record.
     */

    private void compareResults(String xenonRead, String kafkaRecord) {
        if (xenonRead.compareTo(kafkaRecord) == 0) {
            log.info("Comparision result --->> SUCCESS");
        } else {
            log.info("Comparision result --->> FAILURE ");
        }
    }


    /**
     * Function to establish connection to xenon.
     *
     * @param host - host to connect to.
     * @param port - port number to use.
     * @return XenonClient - an instance of this class.
     */
    private XenonClient connectToXenon(String host, Integer port) throws IOException {
        Socket socket = new Socket(host, port);
        XenonClient client = new XenonClient(socket);
        if (!(client.connect())) {
            throw new ConnectException("Connection to Xenon failed.");
        }
        return client;
    }

    /**
     * Function to connect to Xenon and check whether dataset already exists.
     *
     * @param datasetName - Name of the dataset to check.
     * @return XenonClient - an instance of this class.
     */
    private XenonClient checkExist(String datasetName) throws IOException {
        XenonClient xenonClient = connectToXenon("localhost", 41000);
        if (xenonClient.datasetExist(datasetName).exist) {
            xenonClient.datasetRemove(datasetName);
        }
        return xenonClient;
    }


    /**
     * Function responsible to create Kafka record
     * with different values of valueSchema and Object.
     *
     * @param s - schema of record.value() or valueSchema.
     * @param o - record.value() passed as an object.
     * @return new Kafka record.
     */
    private SinkRecord createRecord(Schema s, Object o) {
        Object key = null;
        int partition = 2;
        long offset = 1;
        Schema keySchema = Schema.STRING_SCHEMA;
        SinkRecord record;
        record = new SinkRecord(TOPIC, partition, keySchema, key, s, o, offset);
        return record;
    }


    private Map<String, Integer> createIntMap(Integer limit) {
        Map<String, Integer> mapInt = new HashMap<>();
        for (Integer i = 0; i < limit; i++) {
            String key = "k" + i;
            mapInt.put(key, i);
        }
        return mapInt;
    }
}
