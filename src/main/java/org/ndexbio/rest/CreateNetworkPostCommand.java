package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ndexbio.rest.utils.RidConverter;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CreateNetworkPostCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "POST|ndexNetworkCreate/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public CreateNetworkPostCommand(OServerCommandConfiguration configuration) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkCreate/<database>");

    iRequest.data.commandInfo = "Execute ndex network creation";

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraphNoTx orientGraph = new OrientGraphNoTx(db);
    ndexNetworkService.init(orientGraph);

    try {
      final ObjectMapper objectMapper = new ObjectMapper();

      final JsonNode rootNode = objectMapper.readTree(iRequest.content);
      final JsonNode network = rootNode.get("network");
      final OrientVertex owner = loadOwner(orientGraph, rootNode.get("accountid").asText());
      OrientVertex vNetwork = ndexNetworkService.createNetwork(owner, network, orientGraph);

      orientGraph.commit();

      final ObjectNode result = objectMapper.createObjectNode();
      result.put("jid", convertFromRID((ORID) vNetwork.getId()));
      result.put("ownedBy", rootNode.get("accountid").asText());

      final String resultString = result.toString();

      iResponse
          .send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, resultString, null, true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during network creation", e);
      iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, OHttpUtils.STATUS_INTERNALERROR_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN, e.getMessage(), null, true);
    } finally {
      orientGraph.shutdown();
    }

    return false;
  }

  private OrientVertex loadOwner(OrientBaseGraph orientGraph, String accountid) {
    final OrientVertex vertex = orientGraph.getVertex(RidConverter.convertToRID(accountid));

    if (!vertex.getLabel().equals("xUser")) {
      throw new IllegalStateException("User with id = " + accountid + " is not found.");
    }

    return vertex;
  }

  private String convertFromRID(ORID rid) {
    return rid.toString().replace("#", "C").replace(":", "R");
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
