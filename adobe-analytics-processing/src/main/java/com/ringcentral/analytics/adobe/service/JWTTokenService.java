package com.ringcentral.analytics.adobe.service;

import com.ringcentral.analytics.adobe.utils.ImportOptions;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.stream.Collectors;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import static java.lang.Boolean.TRUE;
import static java.util.Map.entry;

/**
 * To get web token: First step is to create a JSON Web Token (JWT) using your
 * API Key, Technical Account ID, Organisation ID and Private Key.
 * You'll also need to set an expiry time for the JWT,
 * usually only a few minutes (as we'll be exchanging it for an Access Token shortly),
 * plus a metascope for the JWT - a string unique to the API you're accessing.
 * For an access token with an Adobe Analytics metascope, you'll use:
 * https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk
 *
 * With your JWT created, the next step is to exchange it for an Access Token,
 * which is the actual credential we will use to authorise with the API.
 * We'll need the API Key and Client Secret,
 * which we pass along with the JWT to Adobe's JWT exchange endpoint:
 * https://ims-na1.adobelogin.com/ims/exchange/jwt
 * */
public class JWTTokenService {
    private static final Logger LOG = LoggerFactory.getLogger(JWTTokenService.class);
    private final String adobeJwtClientId;
    private final String adobeOrgId;
    private final String adobeTechnicalAccountId;
    private final String adobeKeyPath;
    private final String adobeImsHost;
    private final String adobeMetascopes;


    public JWTTokenService(String adobeJwtClientId, String adobeOrgId, String adobeTechnicalAccountId, String adobeKeyPath, String adobeImsHost, String adobeMetascopes) {
        this.adobeJwtClientId = adobeJwtClientId;
        this.adobeOrgId = adobeOrgId;
        this.adobeTechnicalAccountId = adobeTechnicalAccountId;
        this.adobeKeyPath = adobeKeyPath;
        this.adobeImsHost = adobeImsHost;
        this.adobeMetascopes = adobeMetascopes;
    }

    public JWTTokenService(ImportOptions importOptions) {
        this.adobeJwtClientId = importOptions.getAdobeJwtClientId();
        this.adobeOrgId = importOptions.getAdobeOrgId();
        this.adobeTechnicalAccountId = importOptions.getAdobeTechnicalAccountId();
        this.adobeKeyPath = importOptions.getAdobeKeyPath();
        this.adobeImsHost = importOptions.getAdobeImsHost();
        this.adobeMetascopes = importOptions.getAdobeMetascopes();
    }

    public String getJWTToken() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        // Expiration time in seconds
        Long expirationTime = System.currentTimeMillis() / 1000 + 86400L;
        // Metascopes associated to key
        String metascopes[] = adobeMetascopes.split(",");

        // Secret key as byte array. Secret key file should be in DER encoded format.
        byte[] privateKeyFileContent = Base64.getDecoder().decode(Files.lines(Paths.get(adobeKeyPath))
                .filter(l -> !l.contains("-----"))
                .collect(Collectors.joining()));

        // Read the private key
        RSAPrivateKey privateKey = (RSAPrivateKey)KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(privateKeyFileContent));

        // Create JWT payload
        Map<String, Object> jwtClaims = new HashMap<>(){{
                put( "iss", adobeOrgId);
                put( "sub", adobeTechnicalAccountId);
                put( "exp", expirationTime);
                put( "aud", "https://" + adobeImsHost + "/c/" + adobeJwtClientId);}};

        Arrays.stream(metascopes).forEach(metascope->jwtClaims.put("https://" + adobeImsHost + "/s/" + metascope, TRUE));

        // Create the final JWT token
        String jwtToken = Jwts.builder().setClaims(jwtClaims).signWith(SignatureAlgorithm.RS256, privateKey).compact();
        return jwtToken;
    }
}
