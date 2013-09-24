package org.ndexbio.rest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

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
      return false;
    } finally {
      orientGraph.shutdown();
    }
  }

  private OrientVertex loadOwner(OrientGraph orientGraph, String accountid) {
    final OrientVertex vertex = orientGraph.getVertex(convertToRID(accountid));

    if (!vertex.getLabel().equals("xUser")) {
      throw new RuntimeException("User with id = " + accountid + " is not found.");
    }

    return vertex;
  }

  private ORID convertToRID(String id) {
    final Matcher m = Pattern.compile("^C(\\d*)R(\\d*)$").matcher(id.trim());

    if (m.matches())
      return new ORecordId(Integer.valueOf(m.group(1)), OClusterPositionFactory.INSTANCE.valueOf(m.group(2)));
    else
      throw new RuntimeException(id + " is not valid jid");
  }

  private String convertFromRID(ORID rid) {
    return rid.toString().replace("#", "C").replace(":", "R");
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
