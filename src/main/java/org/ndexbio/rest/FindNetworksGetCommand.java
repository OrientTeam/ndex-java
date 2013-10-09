package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 9/26/13
 */
public class FindNetworksGetCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "GET|ndexNetworkFind/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public FindNetworksGetCommand(OServerCommandConfiguration configuration) {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkFind/<database>");

    iRequest.data.commandInfo = "Execute ndex network find";

    final ObjectMapper objectMapper = new ObjectMapper();
    final JsonNode rootNode = objectMapper.readTree(iRequest.content);

    String searchExpression = rootNode.get("searchExpression").asText();
    if (searchExpression == null)
      searchExpression = "";

    int limit;
    if (rootNode.get("limit") != null)
      limit = rootNode.get("limin").asInt();
    else
      limit = 100;

    int offset;
    if (rootNode.get("offset") != null)
      offset = rootNode.get("offset").asInt();
    else
      offset = 0;

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraphNoTx orientGraph = new OrientGraphNoTx(db);
    ndexNetworkService.init(orientGraph);
    try {

      final JsonNode result = ndexNetworkService.findNetworks(searchExpression, limit, offset, orientGraph, objectMapper);

      final String resultString = result.toString();
      iResponse
          .send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, resultString, null, true);
    } finally {
      orientGraph.shutdown();
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
