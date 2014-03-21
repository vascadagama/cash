/**
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.hive.cassandra.ql.udf;

import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "hex_to_bytes",
    value = "_FUNC_([binary]) - Converts a binary input to a hex string",
    extended = "Takes a binary and returns a String")

public class UDFBytesToHex extends UDF {

  public Text evaluate(BytesWritable byteData){
    String hex = byteData.toString();
    hex = hex.replaceAll(" ", "");
    return new Text(hex);
  }

}

