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
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import static com.levyx.xenon.connector.XenonSinkConnectorConfig.*;
import static org.junit.Assert.*;

/**
 * XenonWrapperTest.java
 * Purpose: Test and Validate the methods in XenonWrapper.java
 */
public class XenonWrapperTest {
    private static XenonClient xenonClient;
    private static String name = "temp";
    private static String schema1 = "{Date:CHAR, Type:CHAR, SymbolID:CHAR,"
            + " SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
            + "Durationms:CHAR, Attribute:CHAR}";
    private static final String schema2 = "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
            + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR,"
            + "     Durationms:CHAR, Attribute:CHAR}";
    private static final XenonClient.DatasetOpenMode[] createMode = {
            XenonClient.DatasetOpenMode.XE_O_CREATE,
            XenonClient.DatasetOpenMode.XE_O_WRITEONLY
    };
    private final Map<String, Object> checkConfig = new XenonSinkConnector()
            .config().parse(buildProps());

    /*Creating a map for connector configurations*/
    private Map<String, String> buildProps() {
        Map<String, String> sampleConfig = new HashMap<>();
        sampleConfig.put(XENON_NAME, "xenon-sink-connector");
        sampleConfig.put(XENON_HOST, "localhost");
        sampleConfig.put(XENON_PORT, "41000");
        sampleConfig.put(XENON_BUFFER_CAPACITY, "164");
        sampleConfig.put(SCHEMA_VERSION, "1");
        sampleConfig.put(TOPICS, "topicA");
        sampleConfig.put(XENON_DATASET_NAME, "temp");
        sampleConfig.put(XENON_DATASET_SCHEMA, "{Date:CHAR, Type:CHAR, SymbolID:CHAR, "
                + "SequenceID:CHAR, BuySell:CHAR, Volume:CHAR, Symbol:CHAR, "
                + "Durationms:CHAR, Attribute:CHAR}");
        return sampleConfig;
    }

    /**
     * Involves validating host and port configuration.
     * Further verifies that socket passed is not null
     * and connection to xenon is a success.
     */
    @Test
    public void testGetXeClient() throws IOException {
        int port = 41000;
        String host = "localhost";
        assertEquals(host, checkConfig.get(XENON_HOST));
        assertEquals(port, checkConfig.get(XENON_PORT));
        Socket clientSocket = new Socket(host, port);
        assertNotNull(clientSocket);
        xenonClient = new XenonClient(clientSocket);
        assertNotNull(xenonClient);
        boolean result = xenonClient.connect();
        assertTrue(result);
    }

    /**
     * Involves validating name, schema and numCores configuration.
     * Further the test verifies that name, schema, numCores
     * and createMode is not null.
     * Finally fanout value is verified and dataset is opened and
     * closed in xenon.
     */
    @Test
    public void testOpenDataSet() throws IOException {
        testGetXeClient();
        assertEquals(name, checkConfig.get(XENON_DATASET_NAME));
        assertEquals(schema1, checkConfig.get(XENON_DATASET_SCHEMA));
        name = (String) checkConfig.get(XENON_DATASET_NAME);
        schema1 = (String) checkConfig.get(XENON_DATASET_SCHEMA);

        assertNotNull(name);
        assertNotNull(schema1);
        assertNotNull(createMode);
        XenonClient.DatasetStatus datasetStatus = xenonClient.datasetOpen(name,
                schema1,
                createMode
                ,0);
        assertNotNull(datasetStatus);
        assertNotNull(datasetStatus.handler);
        assertNotNull(datasetStatus.fanout);
        xenonClient.datasetClose(datasetStatus.handler);
    }

    /**
     * Shows consequence of validation failure of XENON_DATASET_SCHEMA and thus
     * throws AssertionError.
     */
    @Test(expected = AssertionError.class)
    public void testSchemaMismatch() {
        assertEquals(name, checkConfig.get(XENON_DATASET_NAME));
        assertEquals(schema2, checkConfig.get(XENON_DATASET_SCHEMA));
    }

    /**
     * Tests successful datasetRemoval after closing dataset
     * in xenon.
     */
    @Test
    public void testExitAndRemoved() throws IOException {
        testOpenDataSet();
        boolean removed = xenonClient.datasetRemove(name);
        assertTrue(removed);
    }
}