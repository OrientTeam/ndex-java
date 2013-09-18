package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class CreateNetworkPostCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES = { "POST|ndexNetworkCreate/*" };

  private final NdexNetworkService ndexNetworkService;

  public CreateNetworkPostCommand(OServerCommandConfiguration configuration) {
    ndexNetworkService = new NdexNetworkService();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkCreate/<database>");

    iRequest.data.commandInfo = "Execute ndex network creation";

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraph orientGraph = new OrientGraph(db);
    try {
      db.begin();

      final ObjectMapper objectMapper = new ObjectMapper();

      JsonNode rootNode = objectMapper.readTree(iRequest.content);
      ndexNetworkService.createNetwork(null, rootNode, orientGraph);

      orientGraph.commit();
    } finally {
      orientGraph.shutdown();
    }

    iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, "Network created",
        null, true);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
