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

import com.levyx.xenon.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.levyx.xenon.connector.XenonSinkConnectorConfig.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * XenonSinkConnectorTest.java
 * Test and validate methods in XenonSinkConnector.java
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(XenonSinkConnector.class)
public class XenonSinkConnectorTest {
    private XenonSinkConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sampleConfig;

    @Before
    public void setup() {
        connector = new XenonSinkConnector();
        ctx = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "connector");
        sampleConfig.put(XENON_HOST, "f3");
        sampleConfig.put(XENON_PORT, "51000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA,topicB");
        sampleConfig.put(XENON_DATASET_NAME, "test");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
    }

    /**
     * Validates the version of the connector.
     */
    @Test
    public void testVersion() {
        PowerMock.replayAll();
        assertEquals(Version.getVersion(), connector.version());
        PowerMock.verifyAll();
    }

    /**
     * Tests successfully passing configuration map to start()
     */
    @Test
    public void testStartStop() {
        PowerMock.replayAll();
        connector.start(sampleConfig);
        connector.stop();
        PowerMock.verifyAll();
    }

    /**
     * Shows consequence of passing invalid configuration to start().
     * Here the ConfigException error is thrown because we did not pass
     * XENON_NAME.
     */
    @Test(expected = ConfigException.class)
    public void testStartInvalidConnectorName() {
        PowerMock.replayAll();
        sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "");
        sampleConfig.put(XENON_HOST, "f3");
        sampleConfig.put(XENON_PORT, "51000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA,topicB");
        sampleConfig.put(XENON_DATASET_NAME, "temp");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        connector.start(sampleConfig);
        PowerMock.verifyAll();
    }

    /**
     * Validates the Task implementation for this Connector
     */
    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sampleConfig);
        assertEquals(XenonSinkTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }

    /**
     * Validates the maxTasks value (maximum number of configurations to generate)
     * that is passed to the connect framework via the tasks.max property.
     */
    @Test
    public void testTaskConfigs() {
        PowerMock.replayAll();
        connector.start(sampleConfig);
        assertThat(connector.taskConfigs(0), hasSize(0));
        assertThat(connector.taskConfigs(10), hasSize(10));
        PowerMock.verifyAll();
    }

    /**
     * Validates the Configuration parsing done using the sampleConfig map.
     */
    @Test
    public void testConfigParsing() {
        PowerMock.replayAll();
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        List<String> topicList;
        topicList = (List<String>) checkConfig.get(TOPICS);
        assertEquals("connector", checkConfig.get(XENON_NAME));
        assertEquals("f3", checkConfig.get(XENON_HOST));
        assertEquals(51000, checkConfig.get(XENON_PORT));
        assertEquals(164, checkConfig.get(XENON_BUFFER_CAPACITY));
        assertEquals(1, checkConfig.get(SCHEMA_VERSION));
        assertThat(topicList, contains("topicA", "topicB"));
        assertEquals("test", checkConfig.get(XENON_DATASET_NAME));
        assertEquals("{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}", checkConfig.get(XENON_DATASET_SCHEMA));
        PowerMock.verifyAll();
    }

    /**
     * Shows consequence of passing invalid configuration for the Connector to parse.
     * testStartInvalidConnectorName() tested passing invalid configurations to start().
     * Here the ConfigException error is thrown because we did not pass XENON_NAME which
     * is invalid.
     */
    @Test(expected = ConfigException.class)
    public void testConfigParsingEmptyConnectorName() {
        PowerMock.replayAll();
        Map<String, String> sampleConfig = new HashMap<>();
        //XENON_NAME is Empty and thus invalid.
        //String must be non-Empty exception thrown.
        sampleConfig.put(XENON_NAME, "");
        sampleConfig.put(XENON_HOST, "f3");
        sampleConfig.put(XENON_PORT, "51000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA,topicB");
        sampleConfig.put(XENON_DATASET_NAME, "temp");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        PowerMock.verifyAll();
    }

    /**
     * Shows consequence of passing invalid configuration for the Connector to parse.
     * Here the ConfigException error is thrown because we passed an invalid value
     * for XENON_PORT.
     */
    @Test(expected = ConfigException.class)
    public void testConfigParsingInvalidPortValue() {
        PowerMock.replayAll();
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "xenon-sink-connector");
        sampleConfig.put(XENON_HOST, "f3");
        //port value is invalid as value is to be at least 1024.
        sampleConfig.put(XENON_PORT, "1022");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA,topicB");
        sampleConfig.put(XENON_DATASET_NAME, "temp");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        PowerMock.verifyAll();
    }


    /**
     * Shows consequence of passing invalid configuration for the Connector to parse.
     * Here the ConfigException error is thrown because we passed an invalid value
     * for XENON_DATASET_NAME.
     */
    @Test(expected = ConfigException.class)
    public void testConfigParsingInvalidDatasetName() {
        PowerMock.replayAll();
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "xenon-sink-connector");
        sampleConfig.put(XENON_HOST, "f3");
        sampleConfig.put(XENON_PORT, "51000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA,topicB");
        //XENON_DATASET_NAME is Empty and thus invalid.
        //String must be non-Empty exception thrown.
        sampleConfig.put(XENON_DATASET_NAME, "");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        PowerMock.verifyAll();
    }


    /**
     * Shows consequence of passing invalid configuration for the Connector to parse.
     * Here the ConfigException error is thrown because we passed an invalid value
     * for SCHEMA_VERSION.
     */
    @Test(expected = ConfigException.class)
    public void testConfigParsingInvalidSchemaVersion() {
        PowerMock.replayAll();
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "xenon-sink-connector");
        sampleConfig.put(XENON_HOST, "f3");
        sampleConfig.put(XENON_PORT, "51000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        // cannot have null value where we expected int.
        sampleConfig.put(SCHEMA_VERSION, "");
        sampleConfig.put(TOPICS, "topicA,topicB");
        sampleConfig.put(XENON_DATASET_NAME, "temp");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        PowerMock.verifyAll();
    }


    /**
     * Shows consequence of incorrect schema match.
     * Here the ConfigException error is thrown because the value we passed
     * didnot match that assigned to SCHEMA_VERSION.
     */
    @Test(expected = AssertionError.class)
    public void testSchemaVersionMismatch() {
        PowerMock.replayAll();
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(SCHEMA_VERSION, "1");
        Map<String, Object> checkConfig = connector.config().parse(sampleConfig);
        assertEquals(2, checkConfig.get(SCHEMA_VERSION));
        PowerMock.verifyAll();
    }
}