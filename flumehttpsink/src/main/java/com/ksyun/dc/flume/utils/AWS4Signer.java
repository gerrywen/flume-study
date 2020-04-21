package com.ksyun.dc.flume.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ReadLimitInfo;
import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AbstractAWSSigner;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.Presigner;
import com.amazonaws.auth.RegionAwareSigner;
import com.amazonaws.auth.ServiceAwareSigner;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.auth.internal.AWS4SignerRequestParams;
import com.amazonaws.auth.internal.AWS4SignerUtils;
import com.amazonaws.auth.internal.SignerKey;
import com.amazonaws.internal.FIFOCache;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.DateUtils;
import com.amazonaws.util.SdkHttpUtils;
import com.amazonaws.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.auth.internal.SignerConstants.AUTHORIZATION;
import static com.amazonaws.auth.internal.SignerConstants.AWS4_SIGNING_ALGORITHM;
import static com.amazonaws.auth.internal.SignerConstants.AWS4_TERMINATOR;
import static com.amazonaws.auth.internal.SignerConstants.HOST;
import static com.amazonaws.auth.internal.SignerConstants.LINE_SEPARATOR;
import static com.amazonaws.auth.internal.SignerConstants.PRESIGN_URL_MAX_EXPIRATION_SECONDS;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_ALGORITHM;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_CONTENT_SHA256;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_CREDENTIAL;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_DATE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_EXPIRES;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SECURITY_TOKEN;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SIGNATURE;
import static com.amazonaws.auth.internal.SignerConstants.X_AMZ_SIGNED_HEADER;

/**
 * program: flume-study->AWS4Signer
 * description: 亚马逊加密签名
 * author: gerry
 * created: 2020-04-20 16:44
 **/
public class AWS4Signer extends AbstractAWSSigner implements ServiceAwareSigner, RegionAwareSigner, Presigner {

    // 日志打印
    private static final Logger logger = LoggerFactory.getLogger(AWS4Signer.class);

    // 缓存最大数
    private static final int SIGNER_CACHE_MAX_SIZE = 300;

    // 先进先出缓存算法(FIFO)
    private static final FIFOCache<SignerKey> singerCache = new FIFOCache<>(SIGNER_CACHE_MAX_SIZE);

    protected String serviceName;

    protected String regionName;

    protected Date overriddenDate;

    protected boolean doubleUrlEncode;

    public AWS4Signer() {
        this(true);
    }

    public AWS4Signer(boolean doubleUrlEncode) {
        this.doubleUrlEncode = doubleUrlEncode;
    }

    public String getRegionName() {
        return regionName;
    }

    @Override
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }


    public String getServiceName() {
        return serviceName;
    }

    @Override
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setOverriddenDate(Date overriddenDate) {
        this.overriddenDate = overriddenDate;
    }

    public Date getOverriddenDate() {
        return overriddenDate == null ? null : new Date(overriddenDate.getTime());
    }


    @Override
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {
        if (isAnonymous(credentials)) return;
        AWSCredentials sanitizeCredentials = sanitizeCredentials(credentials);
        if (sanitizeCredentials instanceof AWSSessionCredentials) {
            addSessionCredentials(request, (AWSSessionCredentials) sanitizeCredentials);
        }

        final AWS4SignerRequestParams signerParams = new AWS4SignerRequestParams(request, overriddenDate, regionName, serviceName, AWS4_SIGNING_ALGORITHM);

        addHostHeader(request);
        request.addHeader(X_AMZ_DATE, signerParams.getFormattedSigningDateTime());

        String contentSha256 = calculateContentHash(request);

        if ("required".equals(request.getHeaders().get(X_AMZ_CONTENT_SHA256))) {
            request.addHeader(X_AMZ_CONTENT_SHA256, contentSha256);
        }


        final String contentRequest = createCanonicalRequest(request, contentSha256);

        final String stringToSign = createStringToSign(contentRequest, signerParams);

        final byte[] signingKey = deriveSigningKey(sanitizeCredentials, signerParams);

        final byte[] signature = computeSignature(stringToSign, signingKey, signerParams);

        request.addHeader(AUTHORIZATION, buildAuthorizationHeader(request, signature, sanitizeCredentials, signerParams));

        processRequestPayload(request, signature, signingKey, signerParams);
    }

    @Override
    public void presignRequest(SignableRequest<?> request, AWSCredentials credentials, Date date) {
        if (isAnonymous(credentials)) return;

        long expirationInSeconds = generateExpirationDate(date);
        addHostHeader(request);

        AWSCredentials sanitizeCredentials = sanitizeCredentials(credentials);
        if (sanitizeCredentials instanceof AWSSessionCredentials) {
            request.addParameter(X_AMZ_SECURITY_TOKEN, ((AWSSessionCredentials) sanitizeCredentials).getSessionToken());
        }
        final AWS4SignerRequestParams signerRequestParams = new AWS4SignerRequestParams(request, overriddenDate, regionName,
                serviceName, AWS4_SIGNING_ALGORITHM);

        final String timeStamp = AWS4SignerUtils.formatTimestamp(System.currentTimeMillis());
        addPreSignInformationToRequest(request, sanitizeCredentials, signerRequestParams, timeStamp, expirationInSeconds);

        final String contentSha256 = calculateContentHashPresign(request);

        final String contentRequest = createCanonicalRequest(request, contentSha256);

        final String stringToSign = createStringToSign(contentRequest, signerRequestParams);

        final byte[] signingKey = deriveSigningKey(sanitizeCredentials, signerRequestParams);

        final byte[] signature = computeSignature(stringToSign, signingKey, signerRequestParams);

        request.addParameter(X_AMZ_SIGNATURE, BinaryUtils.toHex(signature));

    }

    protected String createCanonicalRequest(SignableRequest<?> request, String contentSha256) {
        final String path = SdkHttpUtils.appendUri(request.getEndpoint().getPath(), request.getResourcePath());

        final StringBuffer stringBuffer = new StringBuffer(request.getHttpMethod().toString());
        stringBuffer.append(LINE_SEPARATOR)
                .append(getCanonicalizedResourcePath(path, doubleUrlEncode))
                .append(LINE_SEPARATOR)
                .append(getCanonicalizedQueryString(request))
                .append(LINE_SEPARATOR)
                .append(getCanonicalizedHeaderString(request))
                .append(LINE_SEPARATOR)
                .append(getSignedHeadersString(request))
                .append(LINE_SEPARATOR).append(contentSha256);
        final String canonicalRequest = stringBuffer.toString();

        logger.info("AWS4 Canonical Request : " + canonicalRequest);
        return canonicalRequest;
    }

    protected String createStringToSign(String contentRequest, AWS4SignerRequestParams signerParams) {

        final StringBuffer stringBuffer = new StringBuffer(signerParams.getSigningAlgorithm());
        stringBuffer.append(LINE_SEPARATOR)
                .append(signerParams.getFormattedSigningDateTime())
                .append(LINE_SEPARATOR)
                .append(signerParams.getScope())
                .append(LINE_SEPARATOR)
                .append(BinaryUtils.toHex(hash(contentRequest)));

        final String stringToSign = stringBuffer.toString();

        logger.info("AWS4 String To Sign : " + stringToSign);
        return stringToSign;
    }

    protected final byte[] deriveSigningKey(AWSCredentials credentials, AWS4SignerRequestParams signerRequestParams) {
        final String cacheKey = computeSigningCacheKeyName(credentials, signerRequestParams);
        final long daysSinceEpochSigningDate = DateUtils.numberOfDaysSinceEpoch(signerRequestParams.getSigningDateTimeMilli());

        SignerKey signerKey = singerCache.get(cacheKey);
        if (signerKey != null) {
            if (daysSinceEpochSigningDate == signerKey.getNumberOfDaysSinceEpoch()) {
                return signerKey.getSigningKey();
            }
        }

        logger.info("Generating a new signing key as the signing key not available in the cache for the date " + TimeUnit.DAYS.toMillis(daysSinceEpochSigningDate));

        byte[] signingKey = newSigningKey(credentials, signerRequestParams.getFormattedSigningDate(),
                signerRequestParams.getRegionName(), signerRequestParams.getServiceName());
        singerCache.add(cacheKey, new SignerKey(daysSinceEpochSigningDate, signingKey));
        return signingKey;
    }


    protected final String computeSigningCacheKeyName(AWSCredentials credentials, AWS4SignerRequestParams signerRequestParams) {
        final StringBuilder hashStringBuilder = new StringBuilder(credentials.getAWSSecretKey());
        return hashStringBuilder.append("-").append(signerRequestParams.getRegionName())
                .append("-").append(signerRequestParams.getServiceName()).toString();
    }

    protected final byte[] computeSignature(String stringToSign, byte[] signingKey, AWS4SignerRequestParams signerRequestParams) {
        return sign(stringToSign.getBytes(StandardCharsets.UTF_8), signingKey, SigningAlgorithm.HmacSHA256);
    }


    private String buildAuthorizationHeader(SignableRequest<?> request, byte[] signature,
                                            AWSCredentials credentials, AWS4SignerRequestParams signerParams) {
        final String signingCredentials = credentials.getAWSAccessKeyId() + "/" + signerParams.getScope();

        final String credential = "Credential=" + signingCredentials;
        final String signerHeaders = "SignedHeaders=" + getSignedHeadersString(request);
        final String signatureHeader = "Signature=" + BinaryUtils.toHex(signature);

        final StringBuilder authStringBuilder = new StringBuilder();
        authStringBuilder.append(AWS4_SIGNING_ALGORITHM).append(" ")
                .append(credential).append(",").append(signerHeaders).append(",").append(signatureHeader);
        return authStringBuilder.toString();
    }


    private void addPreSignInformationToRequest(SignableRequest<?> request, AWSCredentials credentials,
                                                AWS4SignerRequestParams params, String timeStamp, long expirationInSeconds) {
        String stringCredentials = credentials.getAWSAccessKeyId() + "/" + params.getScope();
        request.addParameter(X_AMZ_ALGORITHM, AWS4_SIGNING_ALGORITHM);
        request.addParameter(X_AMZ_DATE, timeStamp);
        request.addParameter(X_AMZ_SIGNED_HEADER, getSignedHeadersString(request));
        request.addParameter(X_AMZ_EXPIRES, String.valueOf(Long.valueOf(expirationInSeconds)));
        request.addParameter(X_AMZ_CREDENTIAL, stringCredentials);
    }


    @Override
    protected void addSessionCredentials(SignableRequest<?> request, AWSSessionCredentials credentials) {
        request.addHeader(X_AMZ_SECURITY_TOKEN, credentials.getSessionToken());
    }


    protected String getCanonicalizedHeaderString(SignableRequest<?> request) {
        final List<String> sortedHeaders = new ArrayList<>(request.getHeaders().keySet());
        sortedHeaders.sort(String.CASE_INSENSITIVE_ORDER);

        final Map<String, String> requestHeaders = request.getHeaders();
        StringBuilder buffer = new StringBuilder();

        for (String header : sortedHeaders) {
            String key = StringUtils.lowerCase(header).replaceAll("\\s+", " ");
            String value = requestHeaders.get(header);

            buffer.append(key).append(":");
            if (value != null) {
                buffer.append(value.replaceAll("\\s+", " "));
            }
            buffer.append("\n");
        }

        return buffer.toString();
    }

    protected String getSignedHeadersString(SignableRequest<?> request) {
        final List<String> sortedHeaders = new ArrayList<>(request.getHeaders().keySet());
        sortedHeaders.sort(String.CASE_INSENSITIVE_ORDER);
        StringBuilder sb = new StringBuilder();
        for (String header : sortedHeaders) {
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(StringUtils.lowerCase(header));
        }
        return sb.toString();
    }

    protected void addHostHeader(SignableRequest<?> request) {
        final URI endpoint = request.getEndpoint();
        final StringBuilder hostHeaderBuilder = new StringBuilder(endpoint.getHost());
        if (SdkHttpUtils.isUsingNonDefaultPort(endpoint)) {
            hostHeaderBuilder.append(":").append(endpoint.getPort());
        }
        request.addHeader(HOST, hostHeaderBuilder.toString());
    }

    protected String calculateContentHash(SignableRequest<?> request) {
        InputStream payloadStream = getBinaryRequestPayloadStreamWithoutQueryParams(request);
        ReadLimitInfo info = request.getReadLimitInfo();
        payloadStream.mark(info == null ? -1 : info.getReadLimit());
        String contentSha256 = BinaryUtils.toHex(hash(payloadStream));
        try {
            payloadStream.reset();
        } catch (IOException e) {
            throw new AmazonClientException("Unable to reset stream after calculating AWS4 signature", e);
        }
        return contentSha256;
    }

    protected void processRequestPayload(SignableRequest<?> request, byte[] signature, byte[] signingKey, AWS4SignerRequestParams signerRequestParams) {
        return;
    }

    protected String calculateContentHashPresign(SignableRequest<?> request) {
        return calculateContentHash(request);
    }


    private boolean isAnonymous(AWSCredentials credentials) {
        return credentials instanceof AnonymousAWSCredentials;
    }

    private long generateExpirationDate(Date expirationDate) {
        long expirationInSeconds = expirationDate != null ? ((expirationDate.getTime() - System.currentTimeMillis()) / 1000)
                : PRESIGN_URL_MAX_EXPIRATION_SECONDS;

        if (expirationInSeconds > PRESIGN_URL_MAX_EXPIRATION_SECONDS) {
            throw new AmazonClientException("Requests that are pre-signed by SigV4 algorithm are valid for at most 7 days . "
                    + "The expiration date set on the current request ["
                    + AWS4SignerUtils.formatTimestamp(expirationDate.getTime()) + "] has exceeded this limit.");
        }
        return expirationInSeconds;
    }

    /**
     * 生成新的加密key
     *
     * @param credentials
     * @param dateStamp
     * @param regionName
     * @param serviceName
     * @return
     */
    private byte[] newSigningKey(AWSCredentials credentials, String dateStamp, String regionName, String serviceName) {
        byte[] kSecret = ("AWS4" + credentials.getAWSSecretKey()).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256);
        byte[] kRegion = sign(regionName, kDate, SigningAlgorithm.HmacSHA256);
        byte[] kService = sign(serviceName, kRegion, SigningAlgorithm.HmacSHA256);
        return sign(AWS4_TERMINATOR, kService, SigningAlgorithm.HmacSHA256);
    }

}
