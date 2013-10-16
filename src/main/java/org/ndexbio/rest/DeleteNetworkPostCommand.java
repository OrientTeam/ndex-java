package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ndexbio.rest.utils.RidConverter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 9/26/13
 */
public class DeleteNetworkPostCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "POST|ndexNetworkDelete/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public DeleteNetworkPostCommand(OServerCommandConfiguration configuration) {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkDelete/<database>");
    iRequest.data.commandInfo = "Execute ndex network deletion";

    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode rootNode = objectMapper.readTree(iRequest.content);

    final String networkId = rootNode.get("networkid").asText();
    final ORID networkRid = RidConverter.convertToRID(networkId);

    OrientGraph orientGraph = null;
    int retries = 0;

    while (true)
      try {
        ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
        orientGraph = new OrientGraph(db);

        ndexNetworkService.init(orientGraph);

        boolean deleted = ndexNetworkService.deleteNetwork(networkRid, orientGraph);
        ObjectNode result = objectMapper.createObjectNode();
        result.put("deleted", deleted);

        iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, result.toString(),
            null, true);

        break;
      } catch (OConcurrentModificationException cme) {
        retries++;

        if (retries > 10)
          throw cme;
      } finally {
        if (orientGraph != null)
          orientGraph.shutdown();
      }

    return false;
  }

  public String[] getNames() {
    return NAMES;
  }

}
