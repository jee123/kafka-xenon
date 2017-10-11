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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.levyx.xenon.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;


/**
 * XenonSinkConnector.java
 */
public class XenonSinkConnector extends SinkConnector {
    private Map<String, String> fproperties;

    /**
     * Get the version of the connector.
     *
     * @return the version,formatted as a String.
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * Start this connector.This method will only be called
     * on a clean Connector(recently initialized).
     *
     * @param props - configuration settings
     */
    @Override
    public void start(Map<String, String> props) throws ConnectException {
        List<String> errorMessages = new ArrayList<>();
        for (ConfigValue v : config().validate(props)) {
            if (!v.errorMessages().isEmpty()) {
                errorMessages.add("Property " + v.name() + " with value " + v.value()
                        + " does not validate: " + v.errorMessages());
            }
        }

        if (!errorMessages.isEmpty()) {
            throw new ConfigException("Configuration does not validate: \n\t" + errorMessages);
        }
        fproperties = props;
    }

    /**
     * Returns a Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return XenonSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks - maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>(0);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(fproperties);
        }
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() throws ConnectException {
    }

    /**
     * Define the configuration for the connector.
     *
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        return XenonSinkConnectorConfig.CONFIG_DEF;
    }
}
