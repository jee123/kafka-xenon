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

import com.levyx.xenon.util.ValidNonEmptyString;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * XenonSinkConnectorConfig.java
 * Purpose: Defining configuration
 * for the connector.
 */
class XenonSinkConnectorConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(XenonSinkConnectorConfig.class);
    static final String XENON_NAME = "name";
    private static final String XENON_NAME_DOC = "name assigned to sink connector";
    static final String DEFAULT_XENON_NAME = "xenon-sink-connector";

    static final String XENON_HOST = "host";
    private static final String XENON_HOST_DOC = "xenon host";
    static final String DEFAULT_XENON_HOST = "localhost";

    static final String XENON_PORT = "port";
    private static final String XENON_PORT_DOC = "xenon port";
    static final int DEFAULT_XENON_PORT = 41000;

    static final String XENON_BUFFER_CAPACITY = "buffer.capacity";
    private static final String XENON_BUFFER_CAPACITY_DOC = "xenon bytebuffer capacity";
    static final int DEFAULT_XENON_BUFFER_CAPACITY = 100;

    static final String SCHEMA_VERSION = "schema.version";
    private static final String SCHEMA_VERSION_DOC = "schema version of records being sent";
    static final int DEFAULT_SCHEMA_VERSION = 0;

    static final String TOPICS = "topics";
    private static final String TOPICS_DOC = "comma separated list of topics to be used "
            + "as source of data";
    static final String DEFAULT_XENON_TOPIC = "xe_topic";

    static final String XENON_DATASET_NAME = "dataset.name";
    private static final String XENON_DATASET_NAME_DOC = "name of the dataset to be "
            + "opened in xenon";
    static final String DEFAULT_XENON_DATASET_NAME = "tmp_dataset";

    static final String XENON_DATASET_SCHEMA = "dataset.schema";
    private static final String XENON_DATASET_SCHEMA_DOC = "schema associated with the dataset";
    static final String DEFAULT_XENON_DATASET_SCHEMA = "tmp_schema";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(XENON_NAME,
                    ConfigDef.Type.STRING,
                    DEFAULT_XENON_NAME,
                    new ValidNonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    XENON_NAME_DOC)
            .define(XENON_HOST,
                    ConfigDef.Type.STRING,
                    DEFAULT_XENON_HOST,
                    ConfigDef.Importance.LOW,
                    XENON_HOST_DOC)
            .define(XENON_PORT,
                    ConfigDef.Type.INT,
                    DEFAULT_XENON_PORT,
                    ConfigDef.Range.atLeast(1024),
                    ConfigDef.Importance.LOW,
                    XENON_PORT_DOC)
            .define(XENON_BUFFER_CAPACITY,
                    ConfigDef.Type.INT,
                    DEFAULT_XENON_BUFFER_CAPACITY,
                    ConfigDef.Importance.MEDIUM,
                    XENON_BUFFER_CAPACITY_DOC)
            .define(SCHEMA_VERSION,
                    ConfigDef.Type.INT,
                    DEFAULT_SCHEMA_VERSION,
                    ConfigDef.Importance.LOW,
                    SCHEMA_VERSION_DOC)
            .define(TOPICS,
                    ConfigDef.Type.LIST,
                    DEFAULT_XENON_TOPIC,
                    ConfigDef.Importance.MEDIUM,
                    TOPICS_DOC)
            .define(XENON_DATASET_NAME,
                    ConfigDef.Type.STRING,
                    DEFAULT_XENON_DATASET_NAME,
                    new ValidNonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    XENON_DATASET_NAME_DOC)
            .define(XENON_DATASET_SCHEMA,
                    ConfigDef.Type.STRING,
                    DEFAULT_XENON_DATASET_SCHEMA,
                    ConfigDef.Importance.HIGH,
                    XENON_DATASET_SCHEMA_DOC);

    final String name;
    final String host;
    final Integer port;
    final Integer bufferCapacity;
    final Integer schemaVersion;
    final List<String> topics;
    final String datasetName;
    final String datasetSchema;

    /**
     * Constructor passing connector configuration to map and creating
     * variables used in other classes.
     */
    XenonSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        name = getString(XENON_NAME);
        host = getString(XENON_HOST);
        port = getInt(XENON_PORT);
        bufferCapacity = getInt(XENON_BUFFER_CAPACITY);
        schemaVersion = getInt(SCHEMA_VERSION);
        topics = getList(TOPICS);
        datasetName = getString(XENON_DATASET_NAME);
        datasetSchema = getString(XENON_DATASET_SCHEMA);
    }

    static void main(String... args) {
        log.info(CONFIG_DEF.toRst());
    }

}
