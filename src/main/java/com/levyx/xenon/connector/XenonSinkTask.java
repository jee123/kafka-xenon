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

import java.util.Collection;
import java.util.Map;

import com.levyx.xenon.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * XenonSinkTask.java
 * Purpose: SinkTask implements Task interface that takes records loaded from
 * Kafka and sends them to Xenon.
 */

public class XenonSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(XenonSinkTask.class);
    private XenonWrapper xenonWrapper;


    /**
     * Get the version of this task.Usually this should be
     * the same as the corresponding Connector class's version.
     *
     * @return the version, formatted as a String.
     */
    @Override
    public String version() {
        return Version.getVersion(); /*returning connector version*/
    }

    /**
     * Start the Task.
     *
     * @param props - initial configuration
     */
    @Override
    public void start(Map<String, String> props) throws ConnectException {

        xenonWrapper = new XenonWrapper(props);
        xenonWrapper.openDataSet();
    }

    /**
     * The SinkTask use this method to create writers for newly assigned
     * partitions in case of partition rebalance.
     *
     * @param partitions - The list of partitions that are now assigned to
     *                   the task (may include partitions previously assigned to the task).
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        log.info("Opening topic Partitions and creating topicPart map");

    }

    /**
     * Put records in sink.Usually this should send the records to the sink
     * asynchronously and immediately return.
     * If this operation fails, the SinkTask may throw a RetriableException to
     * indicate that the framework should attempt to retry the same call again.
     * Other exceptions will cause the task to be stopped immediately.
     * SinkTaskContext.timeout(long) can be used to set the maximum time before
     * the batch will be retried.
     *
     * @param sinkRecords - the set of records to send.
     */
    @Override
    public void put(Collection<SinkRecord> sinkRecords) throws RetriableException {
        try {
            if (sinkRecords.isEmpty()) {
                return;
            }
            xenonWrapper.saveRecords(sinkRecords);
        } catch (ConnectException e) {
            log.error(e.getMessage());
            throw new RetriableException("Unable to put records to Xenon.");
        }
    }

    /**
     * Flush all records that have been put for specified topic partitions.
     *
     * @param offsets - mapping of TopicPartition to committed offset.
     */
    @Override
    public void flush (Map < TopicPartition, OffsetAndMetadata > offsets){
        log.info("inside flush()");
    }

    /**
     * The SinkTask use this method to close writers for partitions that are no
     * longer assigned to the SinkTask.
     *
     * @param partitions - The list of partitions that should be closed.
     */
    @Override
    public void close (Collection < TopicPartition > partitions) {
        log.info("Inside closing partitions.");
    }

    /**
     * Perform any cleanup to stop this task.
     */
    @Override
    public void stop () {
        log.info("Stopping the connector");
        xenonWrapper.close();
    }

}