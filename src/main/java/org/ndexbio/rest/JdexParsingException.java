package org.ndexbio.rest;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class JdexParsingException extends RuntimeException {
  public JdexParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public JdexParsingException(String message) {
    super(message);
  }
}
