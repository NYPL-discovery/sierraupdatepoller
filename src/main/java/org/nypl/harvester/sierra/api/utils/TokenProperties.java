package org.nypl.harvester.sierra.api.utils;

import java.util.Date;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.stereotype.Component;

@Component
public class TokenProperties {

  private String tokenValue;

  private Date tokenExpiration;

  private String tokenType;

  private OAuth2RestTemplate oAuth2RestTemplate;

  public String getTokenValue() {
    return tokenValue;
  }

  public void setTokenValue(String tokenValue) {
    this.tokenValue = tokenValue;
  }

  public Date getTokenExpiration() {
    return tokenExpiration;
  }

  public void setTokenExpiration(Date tokenExpiration) {
    this.tokenExpiration = tokenExpiration;
  }

  public String getTokenType() {
    return tokenType;
  }

  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }

  public OAuth2RestTemplate getOAuth2RestTemplate() {
    return oAuth2RestTemplate;
  }

  public void setoAuth2RestTemplate(OAuth2RestTemplate oAuth2RestTemplate) {
    this.oAuth2RestTemplate = oAuth2RestTemplate;
  }

}
