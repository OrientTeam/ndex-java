package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.ndexbio.rest.utils.RidConverter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class GetNetworkGetCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "GET|ndexNetworkGet/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public GetNetworkGetCommand(OServerCommandConfiguration configuration) {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkGet/<database>");

    iRequest.data.commandInfo = "Execute ndex network retrieval";

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraph orientGraph = new OrientGraph(db);
    try {
      ndexNetworkService.init(orientGraph);
      final String networkId = iRequest.parameters.get("networkid");
      final ORID networkRid = RidConverter.convertToRID(networkId);

      JsonNode network = ndexNetworkService.getNetwork(networkRid, orientGraph);
      final String resultString = network.toString();
      iResponse
          .send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, resultString, null, true);

      return false;
    } finally {
      orientGraph.shutdown();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
