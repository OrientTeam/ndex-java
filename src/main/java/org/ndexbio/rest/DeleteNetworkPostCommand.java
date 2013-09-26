package org.ndexbio.rest;

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
public class DeleteNetworkPostCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]    NAMES              = { "POST|ndexNetworkDelete/*" };

  private final NdexNetworkService ndexNetworkService = NdexNetworkService.INSTANCE;

  public DeleteNetworkPostCommand(OServerCommandConfiguration configuration) {
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: ndexNetworkDelete/<database>");
    iRequest.data.commandInfo = "Execute ndex network deletion";

    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    OrientGraph orientGraph = new OrientGraph(db);
    ndexNetworkService.init(orientGraph);
    try {
      final String networkId = iRequest.parameters.get("networkid");
      final ORID networkRid = RidConverter.convertToRID(networkId);

      ndexNetworkService.deleteNetwork(networkRid, orientGraph);

      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, "Network with id "
          + networkId + " was successfully deleted.", null, true);
    } finally {
      orientGraph.shutdown();
    }
    return false;
  }

  public String[] getNames() {
    return NAMES;
  }

}
