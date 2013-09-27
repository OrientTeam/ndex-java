package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ndexbio.rest.utils.RidConverter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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
public class GetNetworkByNodesGetCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "GET|ndexNetworkGetByNodes/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public GetNetworkByNodesGetCommand(OServerCommandConfiguration configuration) {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkGetByNodes/<database>");

    iRequest.data.commandInfo = "Execute ndex network get by nodes";

    final String networkid = iRequest.parameters.get("networkid");
    final ORID networkRid = RidConverter.convertToRID(networkid);

    int limit;
    if (iRequest.parameters.get("limit") != null)
      limit = Integer.parseInt(iRequest.parameters.get("limit"));
    else
      limit = 100;

    int offset;
    if (iRequest.parameters.get("offset") != null)
      offset = Integer.parseInt(iRequest.parameters.get("offset"));
    else
      offset = 0;

    final ObjectMapper objectMapper = new ObjectMapper();

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraph orientGraph = new OrientGraph(db);
    ndexNetworkService.init(orientGraph);
    try {
      JsonNode network = ndexNetworkService.getNetworkByNodes(networkRid, offset, limit, orientGraph, objectMapper);

      final String resultString = network.toString();
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
