# https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-6
CREATE TEMP FUNCTION ClassifySatelliteRCode(rcode INTEGER) AS (
  CASE
    WHEN rcode = 1 THEN "⚠️dns/rcode:FormErr"
    WHEN rcode = 2 THEN "⚠️dns/rcode:ServFail"
    WHEN rcode = 3 THEN "❗️dns/rcode:NXDomain"
    WHEN rcode = 4 THEN "⚠️dns/rcode:NotImp"
    WHEN rcode = 5 THEN "❗️dns/rcode:Refused"
    WHEN rcode = 6 THEN "⚠️dns/rcode:YXDomain"
    WHEN rcode = 7 THEN "⚠️dns/rcode:YXRRSet"
    WHEN rcode = 8 THEN "⚠️dns/rcode:NXRRSet"
    WHEN rcode = 9 THEN "⚠️dns/rcode:NotAuth"
    WHEN rcode = 10 THEN "⚠️dns/rcode:NotZone"
    WHEN rcode = 11 THEN "⚠️dns/rcode:DSOTYPENI"
    WHEN rcode = 12 THEN "⚠️dns/rcode:Unassigned"
    WHEN rcode = 13 THEN "⚠️dns/rcode:Unassigned"
    WHEN rcode = 14 THEN "⚠️dns/rcode:Unassigned"
    WHEN rcode = 15 THEN "⚠️dns/rcode:Unassigned"
    WHEN rcode = 16 THEN "⚠️dns/rcode:BadVers"
    WHEN rcode = 17 THEN "⚠️dns/rcode:BadSig"
    WHEN rcode = 18 THEN "⚠️dns/rcode:BadKey"
    WHEN rcode = 19 THEN "⚠️dns/rcode:BadTime"
    WHEN rcode = 20 THEN "⚠️dns/rcode:BadMode"
    WHEN rcode = 21 THEN "⚠️dns/rcode:BadAlg"
    WHEN rcode = 22 THEN "⚠️dns/rcode:BadTrunc"
    WHEN rcode = 23 THEN "⚠️dns/rcode:BadCookie"
    ELSE CONCAT("⚠️dns/unknown_rcode:", rcode)
  END
);

CREATE TEMP FUNCTION ClassifySatelliteError(error STRING) AS (
  CASE
    # Satellite v1
    WHEN REGEXP_CONTAINS(error, '"Err": {}') THEN "⚠️read/udp.timeout"
    WHEN REGEXP_CONTAINS(error, '"Err": 90') THEN "⚠️read/dns.msgsize"
    WHEN REGEXP_CONTAINS(error, '"Err": 111') THEN "⚠️read/udp.refused"
    WHEN REGEXP_CONTAINS(error, '"Err": 113') THEN "⚠️read/ip.host_no_route"
    WHEN REGEXP_CONTAINS(error, '"Err": 24') THEN "setup/system_failure" # Too many open files
    WHEN error = "{}" THEN "⚠️dns/unknown" # TODO figure out origin
    WHEN error = "no_answer" THEN "⚠️dns/answer:no_answer"
    #Satellite v2
    WHEN ENDS_WITH(error, "i/o timeout") THEN "⚠️read/udp.timeout"
    WHEN ENDS_WITH(error, "message too long") THEN "⚠️read/dns.msgsize"
    WHEN ENDS_WITH(error, "connection refused") THEN "⚠️read/udp.refused"
    WHEN ENDS_WITH(error, "no route to host") THEN "⚠️read/ip.host_no_route"
    WHEN ENDS_WITH(error, "short read") THEN "⚠️read/dns.msgsize"
    WHEN ENDS_WITH(error, "read: protocol error") THEN "⚠️read/protocol_error"
    WHEN ENDS_WITH(error, "socket: too many open files") THEN "setup/system_failure"
    ELSE CONCAT("⚠️dns/unknown_error:", error)
  END
);

# Classify all errors into a small set of enums
#
# Input is a nullable error string from the raw data
# Source is one of "ECHO", "DISCARD, "HTTP", "HTTPS"
#
# Output is a string of the format "stage/outcome"
# Documentation of this enum is at
# https://github.com/censoredplanet/censoredplanet-analysis/blob/master/docs/tables.md#outcome-classification
CREATE TEMP FUNCTION ClassifyPageFetchError(error STRING) AS (
  CASE
    # System failures
    WHEN ENDS_WITH(error, "address already in use") THEN "setup/system_failure"
    WHEN ENDS_WITH(error, "protocol error") THEN "setup/system_failure"
    WHEN ENDS_WITH(error, "protocol not available") THEN "setup/system_failure" # ipv4 vs 6 error
    WHEN ENDS_WITH(error, "too many open files") THEN "setup/system_failure"

    # Dial failures
    WHEN ENDS_WITH(error, "network is unreachable") THEN "dial/ip.network_unreachable"
    WHEN ENDS_WITH(error, "no route to host") THEN "dial/ip.host_no_route"
    WHEN ENDS_WITH(error, "connection refused") THEN "dial/tcp.refused"
    WHEN ENDS_WITH(error, "context deadline exceeded") THEN "dial/timeout"
    WHEN ENDS_WITH(error, "connect: connection timed out") THEN "dial/timeout"
    WHEN STARTS_WITH(error, "connection reset by peer") THEN "dial/tcp.reset" #no read: or write: prefix in error
    WHEN ENDS_WITH(error, "connect: connection reset by peer") THEN "dial/tcp.reset"
    WHEN ENDS_WITH(error, "getsockopt: connection reset by peer") THEN "dial/tcp.reset"

    # TLS failures
    WHEN REGEXP_CONTAINS(error, "tls:") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "remote error:") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "local error:") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "readLoopPeekFailLocked: <nil>") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "missing ServerKeyExchange message") THEN "tls/tls.failed"
    WHEN ENDS_WITH(error, "no mutual cipher suite") THEN "tls/tls.failed"
    WHEN REGEXP_CONTAINS(error, "TLS handshake timeout") THEN "tls/timeout"

    # Write failures
    WHEN ENDS_WITH(error, "write: connection reset by peer") THEN "write/tcp.reset"
    WHEN ENDS_WITH(error, "write: broken pipe") THEN "write/system"

    # Read failures
    WHEN REGEXP_CONTAINS(error, "request canceled") THEN "read/timeout"
    WHEN ENDS_WITH(error, "i/o timeout") THEN "read/timeout"
    WHEN ENDS_WITH(error, "shutdown: transport endpoint is not connected") THEN "read/system"
    # TODO: for HTTPS this error could potentially also be SNI blocking in the tls stage
    # find a way to diffentiate this case.
    WHEN REGEXP_CONTAINS(error, "read: connection reset by peer") THEN "read/tcp.reset"

    # HTTP content verification failures
    WHEN REGEXP_CONTAINS(error, "unexpected EOF") THEN "read/http.truncated_response"
    WHEN REGEXP_CONTAINS(error, "EOF") THEN "read/http.empty"
    WHEN REGEXP_CONTAINS(error, "http: server closed idle connection") THEN "read/http.truncated_response"
    WHEN ENDS_WITH(error, "trailer header without chunked transfer encoding") THEN "http/http.invalid"
    WHEN ENDS_WITH(error, "response missing Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "bad Content-Length") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "failed to parse Location header") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed HTTP") THEN "http/http.invalid"
    WHEN REGEXP_CONTAINS(error, "malformed MIME") THEN "http/http.invalid"

    # Unknown errors
    ELSE "unknown/unknown"
  END
);

CREATE TEMP FUNCTION InvalidIpType(ip STRING) AS (
  CASE
    WHEN STARTS_WITH(ip, "0.") THEN "❗️ip_invalid:zero"
    WHEN STARTS_WITH(ip, "127.") THEN "❗️ip_invalid:local_host"
    WHEN STARTS_WITH(ip, "10.") THEN "❗️ip_invalid:local_net"
    WHEN NET.IP_TO_STRING(NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(ip), 12)) = "172.16.0.0"  THEN "❗️ip_invalid:local_net"
    WHEN STARTS_WITH(ip, "192.168.") THEN "❗️ip_invalid:local_net"
    ELSE NULL
  END
);

CREATE TEMP FUNCTION MatchesAkamai(answers ANY TYPE) AS (
  (SELECT LOGICAL_OR(REGEXP_CONTAINS(answer.as_name, "Akamai") OR REGEXP_CONTAINS(answer.ip_organization, "Akamai"))
    FROM UNNEST(answers) answer
  )
);

CREATE TEMP FUNCTION AnswersSignature(answers ANY TYPE) AS (
  ARRAY_TO_STRING(ARRAY(
    SELECT DISTINCT
      CASE
        WHEN answer.as_name != "" THEN CONCAT(answer.as_name, ":", answer.ip_organization)
        WHEN answer.asn IS NOT NULL THEN CONCAT("AS", answer.asn)
        ELSE "missing_as_info"
      END
    FROM UNNEST(answers) answer
  ), ",")
);


CREATE TEMP FUNCTION OutcomeString(domain_name STRING,
                                   dns_error STRING,
                                   rcode INTEGER,
                                   answers ANY TYPE) AS (
    CASE 
        WHEN dns_error is NOT NULL AND dns_error != "" AND dns_error != "null" THEN ClassifySatelliteError(dns_error)
        # TODO fix -1 rcodes in v1 data in the pipeline
        WHEN rcode = -1 THEN "⚠️read/udp.timeout"
        WHEN rcode != 0 THEN ClassifySatelliteRCode(rcode)
        WHEN ARRAY_LENGTH(answers) = 0 THEN "⚠️answer:no_answer"
        ELSE IFNULL(
            (SELECT InvalidIpType(answer.ip) FROM UNNEST(answers) answer LIMIT 1),
            CASE
                WHEN (SELECT LOGICAL_OR(answer.matches_control.ip)
                      FROM UNNEST(answers) answer)
                      THEN "✅answer:matches_ip"
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN "✅answer:valid_cert"
                WHEN (SELECT LOGICAL_OR(answer.http_analysis_is_known_blockpage)
                      FROM UNNEST(answers) answer)
                      THEN CONCAT("❗️page:http_blockpage:", answers[OFFSET(0)].http_analysis_page_signature)
                WHEN (SELECT LOGICAL_OR(answer.https_analysis_is_known_blockpage)
                      FROM UNNEST(answers) answer)
                      THEN CONCAT("❗️page:https_blockpage:", answers[OFFSET(0)].https_analysis_page_signature)
                WHEN (SELECT LOGICAL_OR(a.https_tls_cert_matches_domain AND NOT a.https_tls_cert_has_trusted_ca)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️answer:invalid_ca_valid_domain:", answers[OFFSET(0)].https_tls_cert_issuer)
                WHEN (SELECT LOGICAL_AND(NOT a.https_tls_cert_matches_domain)
                      FROM UNNEST(answers) a)
                      THEN CONCAT("❗️answer:cert_not_for_domain:", answers[OFFSET(0)].https_tls_cert_common_name)
                -- We check AS after cert because we've seen (rare) cases of blockpages hosted on the ISP that also hosts Akamai servers.
                WHEN (SELECT LOGICAL_OR(answer.matches_control.asn)
                      FROM UNNEST(answers) answer)
                      THEN "✅answer:matches_asn"
                WHEN MatchesAkamai(answers) THEN "✅answer:matches_akamai"
                ELSE CONCAT("⚠️answer:not_validated:", AnswersSignature(answers))
            END
        )
    END
);

CREATE TEMP FUNCTION ExtraControls(domain STRING) AS (
  domain = 'eecs.umich.edu' OR
  domain = 'cse.engin.umich.edu' OR
  domain = 'captive.apple.com' OR
  domain = 'gstatic.com' OR
  domain = 'massbrowser.cs.umass.edu'
);

CREATE TEMP FUNCTION GoodDomain(domain STRING) AS (
  domain = '33across.com' OR
  domain = '360.cn' OR
  domain = '3dmgame.com' OR
  domain = '76crimes.com' OR
  domain = '911truth.org' OR
  domain = '9gag.com' OR
  domain = 'a.slack-edge.com' OR
  domain = 'abc.go.com' OR
  domain = 'abc.net.au' OR
  domain = 'about.com' OR
  domain = 'about.riot.im' OR
  domain = 'aboutads.info' OR
  domain = 'abs.twimg.com' OR
  domain = 'academia.edu' OR
  domain = 'addons.mozilla.org' OR
  domain = 'addthis.com' OR
  domain = 'addtoany.com' OR
  domain = 'adobe.com' OR
  domain = 'adobe.io' OR
  domain = 'adsrvr.org' OR
  domain = 'afterellen.com' OR
  domain = 'ajax.aspnetcdn.com' OR
  domain = 'akismet.com' OR
  domain = 'alibaba.com' OR
  domain = 'alidns.com' OR
  domain = 'aliexpress.com' OR
  domain = 'aliexpress.ru' OR
  domain = 'alipay.com' OR
  domain = 'aliyun.com' OR
  domain = 'allout.org' OR
  domain = 'alt1-mtalk.google.com' OR
  domain = 'alt2-mtalk.google.com' OR
  domain = 'alt3-mtalk.google.com' OR
  domain = 'alt4-mtalk.google.com' OR
  domain = 'alt5-mtalk.google.com' OR
  domain = 'alt6-mtalk.google.com' OR
  domain = 'alt7-mtalk.google.com' OR
  domain = 'alt8-mtalk.google.com' OR
  domain = 'amazon.co.jp' OR
  domain = 'amazon.es' OR
  domain = 'amazon.in' OR
  domain = 'amazon.it' OR
  domain = 'amazonvideo.com' OR
  domain = 'americanexpress.com' OR
  domain = 'amphetamines.com' OR
  domain = 'ampproject.org' OR
  domain = 'android.apis.google.com' OR
  domain = 'android.clients.google.com' OR
  domain = 'android.com' OR
  domain = 'android.googleapis.com' OR
  domain = 'aol.com' OR
  domain = 'apache.org' OR
  domain = 'aparat.com' OR
  domain = 'api.github.com' OR
  domain = 'api.protonvpn.ch' OR
  domain = 'apis.google.com' OR
  domain = 'apnews.com' OR
  domain = 'app.developer.here.com' OR
  domain = 'app.element.io' OR
  domain = 'app.slack.com' OR
  domain = 'apple.com' OR
  domain = 'apple.news' OR
  domain = 'apps.apple.com' OR
  domain = 'apps.crowdtangle.com' OR
  domain = 'appsflyer.com' OR
  domain = 'appspot.com' OR
  domain = 'ar.m.wikipedia.org' OR
  domain = 'ar.wikipedia.org' OR
  domain = 'arcgis.com' OR
  domain = 'archive.is' OR
  domain = 'archiveofourown.org' OR
  domain = 'arnebrachhold.de' OR
  domain = 'arxiv.org' OR
  domain = 'assets.gitlab-static.net' OR
  domain = 'atl-paas.net' OR
  domain = 'atlassian.com' OR
  domain = 'atlassian.net' OR
  domain = 'att.com' OR
  domain = 'att.net' OR
  domain = 'attrition.org' OR
  domain = 'avast.com' OR
  domain = 'azure.com' OR
  domain = 'b-cdn.net' OR
  domain = 'badoo.com' OR
  domain = 'baidu.com' OR
  domain = 'bandcamp.com' OR
  domain = 'bankofamerica.com' OR
  domain = 'bbc.co.uk' OR
  domain = 'bbc.com' OR
  domain = 'beeg.com' OR
  domain = 'behance.net' OR
  domain = 'berkeley.edu' OR
  domain = 'beyondexgay.com' OR
  domain = 'bi.org' OR
  domain = 'bilibili.com' OR
  domain = 'binance.com' OR
  domain = 'bing.com' OR
  domain = 'bintray.com' OR
  domain = 'bit.ly' OR
  domain = 'bitbucket.org' OR
  domain = 'bitcoin.org' OR
  domain = 'bitly.com' OR
  domain = 'blog.mozilla.org' OR
  domain = 'blogger.com' OR
  domain = 'blogmarks.net' OR
  domain = 'blogspot.com' OR
  domain = 'bloomberg.com' OR
  domain = 'bmj.com' OR
  domain = 'boingboing.net' OR
  domain = 'booking.com' OR
  domain = 'bouncer.ooni.io' OR
  domain = 'box.com' OR
  domain = 'brave.com' OR
  domain = 'britannica.com' OR
  domain = 'btggaming.com' OR
  domain = 'businessinsider.com' OR
  domain = 'businesswire.com' OR
  domain = 'buzzfeed.com' OR
  domain = 'ca.gov' OR
  domain = 'ca.wikipedia.org' OR
  domain = 'calendly.com' OR
  domain = 'cambridge.org' OR
  domain = 'captive.apple.com' OR
  domain = 'carnegieendowment.org' OR
  domain = 'cbc.ca' OR
  domain = 'cbsnews.com' OR
  domain = 'cc.bingj.com' OR
  domain = 'cctv.com' OR
  domain = 'cdc.gov' OR
  domain = 'cdn-client.medium.com' OR
  domain = 'cdn.ampproject.org' OR
  domain = 'cdn.fbsbx.com' OR
  domain = 'cdn.jsdelivr.net' OR
  domain = 'cdnjs.cloudflare.com' OR
  domain = 'cdnjs.com' OR
  domain = 'censoredplanet.org' OR
  domain = 'censorship.no' OR
  domain = 'change.org' OR
  domain = 'chase.com' OR
  domain = 'chaturbate.com' OR
  domain = 'chess.com' OR
  domain = 'cisco.com' OR
  domain = 'cites.org' OR
  domain = 'clients3.google.com' OR
  domain = 'clients5.google.com' OR
  domain = 'cloudflare-dns.com' OR
  domain = 'cloudflare.com' OR
  domain = 'cloudns.net' OR
  domain = 'clubhouse.pubnub.com' OR
  domain = 'cnbc.com' OR
  domain = 'cnblogs.com' OR
  domain = 'cnet.com' OR
  domain = 'cnki.net' OR
  domain = 'cnn.com' OR
  domain = 'cocaine.org' OR
  domain = 'code.jquery.com' OR
  domain = 'codeload.github.com' OR
  domain = 'coinmarketcap.com' OR
  domain = 'columbia.edu' OR
  domain = 'commons.wikimedia.org' OR
  domain = 'company.wizards.com' OR
  domain = 'connect.rom.miui.com' OR
  domain = 'connectivitycheck.android.com' OR
  domain = 'connectivitycheck.gstatic.com' OR
  domain = 'constantcontact.com' OR
  domain = 'consultant.ru' OR
  domain = 'cornell.edu' OR
  domain = 'coronavirus-map.com' OR
  domain = 'coronavirus.jhu.edu' OR
  domain = 'coursera.org' OR
  domain = 'covidtracking.com' OR
  domain = 'cpanel.net' OR
  domain = 'crashlytics.com' OR
  domain = 'creativecommons.org' OR
  domain = 'criteo.com' OR
  domain = 'csdn.net' OR
  domain = 'cyber.harvard.edu' OR
  domain = 'dailymail.co.uk' OR
  domain = 'dashjr.org' OR
  domain = 'daum.net' OR
  domain = 'db.com' OR
  domain = 'dcinside.com' OR
  domain = 'de.wikipedia.org' OR
  domain = 'debian.org' OR
  domain = 'dell.com' OR
  domain = 'deloitte.com' OR
  domain = 'detectportal.firefox.com' OR
  domain = 'deviantart.com' OR
  domain = 'device-provisioning.googleapis.com' OR
  domain = 'digg.com' OR
  domain = 'digicert.com' OR
  domain = 'digital.globalclimatestrike.net' OR
  domain = 'digitalocean.com' OR
  domain = 'discord.com' OR
  domain = 'discord.gg' OR
  domain = 'discordapp.com' OR
  domain = 'disneyplus.com' OR
  domain = 'dit.whatsapp.net' OR
  domain = 'dl.bintray.com' OR
  domain = 'dl.google.com' OR
  domain = 'dmm.co.jp' OR
  domain = 'dns.adguard.com' OR
  domain = 'dns.google' OR
  domain = 'dns.quad9.net' OR
  domain = 'docs.google.com' OR
  domain = 'doh.dns.sb' OR
  domain = 'doi.org' OR
  domain = 'domainmarket.com' OR
  domain = 'doubleclick.net' OR
  domain = 'douyin.com' OR
  domain = 'douyu.com' OR
  domain = 'download-installer.cdn.mozilla.net' OR
  domain = 'download.cnet.com' OR
  domain = 'dreamhost.com' OR
  domain = 'dribbble.com' OR
  domain = 'dropbox.com' OR
  domain = 'drugs-forum.com' OR
  domain = 'duckdns.org' OR
  domain = 'duckduckgo.com' OR
  domain = 'duolingo.com' OR
  domain = 'dw.com' OR
  domain = 'dynect.net' OR
  domain = 'ea.com' OR
  domain = 'earth.google.com' OR
  domain = 'ebay.co.uk' OR
  domain = 'ebay.com' OR
  domain = 'economist.com' OR
  domain = 'edge-chat.instagram.com' OR
  domain = 'edge-mqtt.facebook.com' OR
  domain = 'eepurl.com' OR
  domain = 'element.io' OR
  domain = 'elpha.com' OR
  domain = 'elsevier.com' OR
  domain = 'en.m.wikipedia.org' OR
  domain = 'en.miui.com' OR
  domain = 'en.wikipedia.org' OR
  domain = 'encrypted-tbn0.gstatic.com' OR
  domain = 'engadget.com' OR
  domain = 'envato.com' OR
  domain = 'epa.gov' OR
  domain = 'epicgames.com' OR
  domain = 'es.m.wikipedia.org' OR
  domain = 'es.wikipedia.org' OR
  domain = 'eset.com' OR
  domain = 'espn.com' OR
  domain = 'etherscan.io' OR
  domain = 'etsy.com' OR
  domain = 'europa.eu' OR
  domain = 'europedatingonline.com' OR
  domain = 'eventbrite.com' OR
  domain = 'evernote.com' OR
  domain = 'exacttarget.com' OR
  domain = 'experience.arcgis.com' OR
  domain = 'explorer.ooni.org' OR
  domain = 'external-content.duckduckgo.com' OR
  domain = 'external.xx.fbcdn.net' OR
  domain = 'facebook.com' OR
  domain = 'fandom.com' OR
  domain = 'fastcompany.com' OR
  domain = 'fastly.net' OR
  domain = 'fb.com' OR
  domain = 'fb.me' OR
  domain = 'fbcdn.net' OR
  domain = 'fc2.com' OR
  domain = 'fcm.googleapis.com' OR
  domain = 'fda.gov' OR
  domain = 'fedex.com' OR
  domain = 'fedoramagazine.org' OR
  domain = 'feedburner.com' OR
  domain = 'figma.com' OR
  domain = 'firebaseinstallations.googleapis.com' OR
  domain = 'fireeye.com' OR
  domain = 'fleegle.mixmin.net' OR
  domain = 'flickr.com' OR
  domain = 'flipkart.com' OR
  domain = 'forbes.com' OR
  domain = 'force.com' OR
  domain = 'ford.com' OR
  domain = 'foreignpolicy.com' OR
  domain = 'fortune.com' OR
  domain = 'forum.grasscity.com' OR
  domain = 'foxnews.com' OR
  domain = 'fr.wikipedia.org' OR
  domain = 'free.fr' OR
  domain = 'freedomhouse.org' OR
  domain = 'freemuse.org' OR
  domain = 'freenetproject.org' OR
  domain = 'freespeechdebate.com' OR
  domain = 'frontlineaids.org' OR
  domain = 'ft.com' OR
  domain = 'fteproxy.org' OR
  domain = 'gandi.net' OR
  domain = 'gcdn.co' OR
  domain = 'geeksforgeeks.org' OR
  domain = 'getlantern.org' OR
  domain = 'getoutline.org' OR
  domain = 'getpocket.com' OR
  domain = 'gisanddata.maps.arcgis.com' OR
  domain = 'gist.github.com' OR
  domain = 'git.io' OR
  domain = 'github.com' OR
  domain = 'github.io' OR
  domain = 'gitlab.com' OR
  domain = 'globalpressjournal.com' OR
  domain = 'globo.com' OR
  domain = 'gmail.com' OR
  domain = 'gnu.org' OR
  domain = 'gofundme.com' OR
  domain = 'goo.gl' OR
  domain = 'goodreads.com' OR
  domain = 'google-analytics.com' OR
  domain = 'google.ca' OR
  domain = 'google.cn' OR
  domain = 'google.co.in' OR
  domain = 'google.co.jp' OR
  domain = 'google.co.th' OR
  domain = 'google.co.uk' OR
  domain = 'google.com' OR
  domain = 'google.com.br' OR
  domain = 'google.com.hk' OR
  domain = 'google.com.mx' OR
  domain = 'google.com.sg' OR
  domain = 'google.com.tr' OR
  domain = 'google.com.tw' OR
  domain = 'google.de' OR
  domain = 'google.es' OR
  domain = 'google.fr' OR
  domain = 'google.it' OR
  domain = 'google.org' OR
  domain = 'google.ru' OR
  domain = 'googledomains.com' OR
  domain = 'googlesyndication.com' OR
  domain = 'googleweblight.com' OR
  domain = 'grammarly.com' OR
  domain = 'gravatar.com' OR
  domain = 'greenhost.net' OR
  domain = 'groups.google.com' OR
  domain = 'gstatic.com' OR
  domain = 'guardster.com' OR
  domain = 'hackerspaces.org' OR
  domain = 'hangouts.google.com' OR
  domain = 'harvard.edu' OR
  domain = 'hbo.com' OR
  domain = 'hbomax.com' OR
  domain = 'hbr.org' OR
  domain = 'healthline.com' OR
  domain = 'heroin.org' OR
  domain = 'herokuapp.com' OR
  domain = 'hilton.com' OR
  domain = 'hinet.net' OR
  domain = 'hola.org' OR
  domain = 'hornet.com' OR
  domain = 'hotjar.com' OR
  domain = 'hotstar.com' OR
  domain = 'howtogrowmarijuana.com' OR
  domain = 'hp.com' OR
  domain = 'huawei.com' OR
  domain = 'hubspot.com' OR
  domain = 'huffingtonpost.com' OR
  domain = 'huffpost.com' OR
  domain = 'hugedomains.com' OR
  domain = 'hulu.com' OR
  domain = 'huya.com' OR
  domain = 'i.gr-assets.com' OR
  domain = 'i.instagram.com' OR
  domain = 'i.pinimg.com' OR
  domain = 'i.ytimg.com' OR
  domain = 'ibm.com' OR
  domain = 'icao.maps.arcgis.com' OR
  domain = 'icloud.com' OR
  domain = 'ifacetimeapp.com' OR
  domain = 'ikea.com' OR
  domain = 'ikhwanonline.com' OR
  domain = 'ilga.org' OR
  domain = 'ilovepdf.com' OR
  # always bad
  # domain = 'im0-tub-com.yandex.net' OR
  domain = 'imdb.com' OR
  domain = 'imgur.com' OR
  domain = 'immunet.com' OR
  domain = 'imo.im' OR
  domain = 'inc.com' OR
  domain = 'independent.co.uk' OR
  domain = 'indiatimes.com' OR
  domain = 'instagram.com' OR
  domain = 'int.soccerway.com' OR
  domain = 'intel.com' OR
  domain = 'intermedia.net' OR
  domain = 'internationalfamilyequalityday.org' OR
  domain = 'intuit.com' OR
  domain = 'investopedia.com' OR
  domain = 'ipi.media' OR
  domain = 'iqiyi.com' OR
  domain = 'irs.gov' OR
  domain = 'islamonline.net' OR
  domain = 'issuu.com' OR
  domain = 'it.wikipedia.org' OR
  domain = 'ixigua.com' OR
  domain = 'iyfglobal.org' OR
  domain = 'ja.wikipedia.org' OR
  domain = 'jainworld.com' OR
  domain = 'jd.com' OR
  domain = 'jimdo.com' OR
  domain = 'jobsatamazon.co.uk' OR
  domain = 'joinmastodon.org' OR
  domain = 'jotform.com' OR
  domain = 'jquery.com' OR
  domain = 'kaspersky.com' OR
  domain = 'kh.google.com' OR
  domain = 'khms0.google.com' OR
  domain = 'khms1.google.com' OR
  domain = 'kiwifarms.net' OR
  domain = 'kohls.com' OR
  domain = 'krishna.com' OR
  domain = 'lalgbtcenter.org' OR
  domain = 'launchpad.net' OR
  domain = 'lencr.org' OR
  domain = 'lens.google.com' OR
  domain = 'letsencrypt.org' OR
  domain = 'lgbt.foundation' OR
  domain = 'lgbtnewsnow.org' OR
  domain = 'lh3.ggpht.com' OR
  domain = 'lh3.googleusercontent.com' OR
  domain = 'lh4.ggpht.com' OR
  domain = 'libgen.gs' OR
  domain = 'libgen.me' OR
  domain = 'libgen.xyz' OR
  domain = 'line.me' OR
  domain = 'linkedin.com' OR
  domain = 'linktr.ee' OR
  domain = 'linode.com' OR
  domain = 'livejournal.com' OR
  domain = 'livestream.com' OR
  domain = 'liveuamap.com' OR
  domain = 'loc.gov' OR
  domain = 'locaweb.com.br' OR
  domain = 'login.live.com' OR
  domain = 'lookaside.facebook.com' OR
  domain = 'mail.ru' OR
  domain = 'mail.yahoo.com' OR
  domain = 'mail.yandex.ru' OR
  domain = 'mailchi.mp' OR
  domain = 'mailchimp.com' OR
  domain = 'mainnet.infura.io' OR
  domain = 'maps.gstatic.com' OR
  domain = 'marca.com' OR
  domain = 'marketwatch.com' OR
  domain = 'marriott.com' OR
  domain = 'mashable.com' OR
  domain = 'mask-api.icloud.com' OR
  domain = 'mastodon.cloud' OR
  domain = 'mastodon.sdf.org' OR
  domain = 'matrix-client.matrix.org' OR
  domain = 'matrix.org' OR
  domain = 'matrix.to' OR
  domain = 'mattermost.com' OR
  domain = 'mayoclinic.org' OR
  domain = 'mcafee.com' OR
  domain = 'mckinsey.com' OR
  domain = 'mdpi.com' OR
  domain = 'media-arn2-1.cdn.whatsapp.net' OR
  domain = 'media.net' OR
  domain = 'media0.giphy.com' OR
  domain = 'media3.giphy.com' OR
  domain = 'mediafire.com' OR
  domain = 'medicalnewstoday.com' OR
  domain = 'medium.com' OR
  domain = 'medpot.net' OR
  domain = 'meetup.com' OR
  domain = 'mega.nz' OR
  domain = 'merriam-webster.com' OR
  domain = 'messages.google.com' OR
  domain = 'messagevortex.net' OR
  domain = 'messenger.com' OR
  domain = 'metasploit.com' OR
  domain = 'mi.com' OR
  domain = 'microsoft.com' OR
  domain = 'miro.medium.com' OR
  domain = 'mit.edu' OR
  domain = 'miui.com' OR
  domain = 'mmg.whatsapp.net' OR
  domain = 'mobile.element.io' OR
  domain = 'mozilla.org' OR
  domain = 'msn.com' OR
  domain = 'mstdn.io' OR
  domain = 'mstdn.jp' OR
  domain = 'mtalk-dev.google.com' OR
  domain = 'mtalk-staging.google.com' OR
  domain = 'mtalk.google.com' OR
  domain = 'mtalk4.google.com' OR
  domain = 'mts.ru' OR
  domain = 'multimedia.scmp.com' OR
  domain = 'multiproxy.org' OR
  domain = 'mysql.com' OR
  domain = 'name.com' OR
  domain = 'nasa.gov' OR
  domain = 'nationalgeographic.com' OR
  domain = 'nature.com' OR
  domain = 'nbcnews.com' OR
  domain = 'ncac.org' OR
  domain = 'nessus.org' OR
  domain = 'nest.com' OR
  domain = 'netflix.com' OR
  domain = 'newrelic.com' OR
  domain = 'news.google.com' OR
  domain = 'newyorker.com' OR
  domain = 'nextstrain.org' OR
  domain = 'nginx.com' OR
  domain = 'nic.ru' OR
  domain = 'nike.com' OR
  domain = 'nintendo.com' OR
  domain = 'noaa.gov' OR
  domain = 'nordvpn.com' OR
  domain = 'notifications-pa.googleapis.com' OR
  domain = 'notion.so' OR
  domain = 'npr.org' OR
  domain = 'nsw2u.com' OR
  domain = 'ntp.org' OR
  domain = 'nvidia.com' OR
  domain = 'nytimes.com' OR
  domain = 'o0.ingest.sentry.io' OR
  domain = 'occupystreams.org' OR
  domain = 'ok.ru' OR
  domain = 'okta.com' OR
  domain = 'onetag-sys.com' OR
  domain = 'onlyfans.com' OR
  domain = 'ooni.github.io' OR
  domain = 'ooni.org' OR
  domain = 'opendns.com' OR
  domain = 'openobservatory.slack.com' OR
  domain = 'openvpn.net' OR
  domain = 'opera.com' OR
  domain = 'opportunity.org' OR
  domain = 'optimizely.com' OR
  domain = 'oracle.com' OR
  domain = 'oreilly.com' OR
  domain = 'oup.com' OR
  domain = 'outbrain.com' OR
  domain = 'outlook.com' OR
  domain = 'outlook.live.com' OR
  domain = 'ovh.net' OR
  domain = 'ox.ac.uk' OR
  domain = 'paloaltonetworks.com' OR
  domain = 'panties.com' OR
  domain = 'patreon.com' OR
  domain = 'paypal.com' OR
  domain = 'pbs.org' OR
  domain = 'pbs.twimg.com' OR
  domain = 'pflag.org' OR
  domain = 'photos.google.com' OR
  domain = 'php.net' OR
  domain = 'pingomatic.com' OR
  domain = 'pinterest.com' OR
  domain = 'pirateparty.org.au' OR
  domain = 'pixelfed.social' OR
  domain = 'pixiv.net' OR
  domain = 'pki.goog' OR
  domain = 'plan-uk.org' OR
  domain = 'play-lh.googleusercontent.com' OR
  domain = 'play.google.com' OR
  domain = 'play.googleapis.com' OR
  domain = 'playstation.com' OR
  domain = 'plesk.com' OR
  domain = 'pps.whatsapp.net' OR
  domain = 'preview.redd.it' OR
  domain = 'pridesource.com' OR
  domain = 'primevideo.com' OR
  domain = 'princeton.edu' OR
  domain = 'prnewswire.com' OR
  domain = 'protonmail.com' OR
  domain = 'protonvpn.com' OR
  domain = 'proxytools.sourceforge.net' OR
  domain = 'psiphon.ca' OR
  domain = 'psu.edu' OR
  domain = 'pt.m.wikipedia.org' OR
  domain = 'pubmatic.com' OR
  domain = 'python.org' OR
  domain = 'qidian.com' OR
  domain = 'qph.fs.quoracdn.net' OR
  domain = 'qq.com' OR
  domain = 'qsbr.fs.quoracdn.net' OR
  domain = 'qualtrics.com' OR
  domain = 'quora.com' OR
  domain = 'quran.com' OR
  domain = 'rackspace.com' OR
  domain = 'radiofreetexas.com' OR
  domain = 'rakuten.co.jp' OR
  domain = 'rambler.ru' OR
  domain = 'raseef22-net.cdn.ampproject.org' OR
  domain = 'raseef22.net' OR
  domain = 'raw.githubusercontent.com' OR
  domain = 'rayjump.com' OR
  domain = 'rbc.ru' OR
  domain = 'reddit.com' OR
  domain = 'redhat.com' OR
  domain = 'reg.ru' OR
  domain = 'reliefweb.int' OR
  domain = 'repo1.maven.org' OR
  domain = 'ria.ru' OR
  domain = 'ring.com' OR
  domain = 'riotgames.com' OR
  domain = 'roblox.com' OR
  domain = 'roche.com' OR
  domain = 'roku.com' OR
  domain = 'rsf.org' OR
  domain = 'rt.ru' OR
  domain = 'ru.wikipedia.org' OR
  domain = 'rubiconproject.com' OR
  domain = 's.gr-assets.com' OR
  domain = 's.pinimg.com' OR
  domain = 'sagepub.com' OR
  domain = 'salesforce.com' OR
  domain = 'samizdat.is' OR
  domain = 'samsung.com' OR
  domain = 'savefrom.net' OR
  domain = 'sberbank.ru' OR
  domain = 'sci-hub.se' OR
  domain = 'sciencemag.org' OR
  domain = 'scontent-ams4-1.cdninstagram.com' OR
  domain = 'scontent-frt3-2.cdninstagram.com' OR
  domain = 'scontent.cdninstagram.com' OR
  domain = 'scontent.xx.fbcdn.net' OR
  domain = 'scribd.com' OR
  domain = 'search.aol.com' OR
  domain = 'search.brave.com' OR
  domain = 'secondlife.com' OR
  domain = 'secure.avaaz.org' OR
  domain = 'secure.flickr.com' OR
  domain = 'securevpn.im' OR
  domain = 'sentry.io' OR
  domain = 'service-now.com' OR
  domain = 'shadowsocks.org' OR
  domain = 'shein.com' OR
  domain = 'shopify.com' OR
  domain = 'signal.org' OR
  domain = 'simple.wikipedia.org' OR
  domain = 'sina.com.cn' OR
  domain = 'siriusxm.com' OR
  domain = 'site.voicepulse.com' OR
  domain = 'sites.google.com' OR
  domain = 'skype.com' OR
  domain = 'slack-imgs.com' OR
  domain = 'slack.com' OR
  domain = 'slashdot.org' OR
  domain = 'slate.com' OR
  domain = 'slideshare.net' OR
  domain = 'snapchat.com' OR
  domain = 'so.com' OR
  domain = 'sogou.com' OR
  domain = 'sohu.com' OR
  domain = 'sophos.com' OR
  domain = 'soundcloud.com' OR
  domain = 'sourceforge.net' OR
  domain = 'spankbang.com' OR
  domain = 'speedify.com' OR
  domain = 'speedtest.net' OR
  domain = 'spideroak.com' OR
  domain = 'spotify.com' OR
  domain = 'sputniknews.com' OR
  domain = 'spys.one' OR
  domain = 'squarespace.com' OR
  domain = 'st1.zoom.us' OR
  domain = 'stackexchange.com' OR
  domain = 'stackoverflow.com' OR
  domain = 'stanford.edu' OR
  domain = 'startpage.com' OR
  domain = 'statcounter.com' OR
  domain = 'state.gov' OR
  domain = 'static.cdninstagram.com' OR
  domain = 'static.xx.fbcdn.net' OR
  domain = 'staticxx.facebook.com' OR
  domain = 'statista.com' OR
  domain = 'stats.bls.gov' OR
  domain = 'steamcommunity.com' OR
  domain = 'steampowered.com' OR
  domain = 'store.steampowered.com' OR
  domain = 'storymaps.arcgis.com' OR
  domain = 'stripe.com' OR
  domain = 'strongvpn.com' OR
  domain = 'styles.redditmedia.com' OR
  domain = 'subscene.com' OR
  domain = 'sun.com' OR
  domain = 'support.therapytribe.com' OR
  domain = 'surfshark.com' OR
  domain = 'surveymonkey.com' OR
  domain = 'swift.org' OR
  domain = 'swrve.com' OR
  domain = 'sxyprn.com' OR
  domain = 'taboola.com' OR
  domain = 'tandfonline.com' OR
  domain = 'taobao.com' OR
  domain = 'target.com' OR
  domain = 'teams.microsoft.com' OR
  domain = 'teamviewer.com' OR
  domain = 'techcrunch.com' OR
  domain = 'technorati.com' OR
  domain = 'ted.com' OR
  domain = 'telegra.ph' OR
  domain = 'telegram.me' OR
  domain = 'telegraph.co.uk' OR
  domain = 'tencent.com' OR
  domain = 'th.bing.com' OR
  domain = 'theatlantic.com' OR
  domain = 'theconversation.com' OR
  domain = 'theguardian.com' OR
  domain = 'thehackernews.com' OR
  domain = 'theintercept.com' OR
  domain = 'themeforest.net' OR
  domain = 'thepiratebay.org' OR
  domain = 'theverge.com' OR
  domain = 'tiktok.com' OR
  domain = 'time.com' OR
  domain = 'timesofindia.indiatimes.com' OR
  domain = 'timeweb.ru' OR
  domain = 'tinder.com' OR
  domain = 'tinyurl.com' OR
  domain = 'tmall.com' OR
  domain = 'torah.org' OR
  domain = 'toutiao.com' OR
  domain = 'tradingview.com' OR
  domain = 'transequality.org' OR
  domain = 'translate.google.com' OR
  domain = 'transparencyreport.google.com' OR
  domain = 'trashy.com' OR
  domain = 'trello.com' OR
  domain = 'trendmicro.com' OR
  domain = 'triller.co' OR
  domain = 'triviasecurity.net' OR
  domain = 'tumblr.com' OR
  domain = 'turbobit.net' OR
  domain = 'twitch.tv' OR
  domain = 'twitter.com' OR
  domain = 'tx.me' OR
  domain = 'typeform.com' OR
  domain = 'tyt.com' OR
  domain = 'uci.edu' OR
  domain = 'ucla.edu' OR
  domain = 'udemy.com' OR
  domain = 'ui.com' OR
  domain = 'ultrasurf.us' OR
  domain = 'umich.edu' OR
  domain = 'umn.edu' OR
  domain = 'un.org' OR
  domain = 'unity3d.com' OR
  domain = 'unpo.org' OR
  domain = 'untappd.com' OR
  domain = 'uol.com.br' OR
  domain = 'upenn.edu' OR
  domain = 'upload.twitter.com' OR
  domain = 'upload.wikimedia.org' OR
  domain = 'uploaded.net' OR
  domain = 'ups.com' OR
  domain = 'usatoday.com' OR
  domain = 'va.gov' OR
  domain = 'valence.community' OR
  domain = 'varzesh3.com' OR
  domain = 'vector.im' OR
  domain = 'veracrypt.fr' OR
  domain = 'vho.org' OR
  domain = 'vice.com' OR
  domain = 'video.cdninstagram.com' OR
  domain = 'video.google.com' OR
  domain = 'video.twimg.com' OR
  domain = 'vimeo.com' OR
  domain = 'visualstudio.com' OR
  domain = 'vk.com' OR
  domain = 'vkontakte.ru' OR
  domain = 'vox.com' OR
  domain = 'w3.org' OR
  domain = 'w3schools.com' OR
  domain = 'wa.me' OR
  domain = 'walmart.com' OR
  domain = 'washington.edu' OR
  domain = 'washingtonpost.com' OR
  domain = 'weather.com' OR
  domain = 'web.archive.org' OR
  domain = 'web.facebook.com' OR
  domain = 'web.wechat.com' OR
  domain = 'webcache.googleusercontent.com' OR
  domain = 'webex.com' OR
  domain = 'wedo.org' OR
  domain = 'weebly.com' OR
  domain = 'weedmaps.com' OR
  domain = 'weforum.org' OR
  domain = 'weibo.com' OR
  domain = 'wellsfargo.com' OR
  domain = 'wetransfer.com' OR
  domain = 'whatsapp.com' OR
  domain = 'whatsapp.net' OR
  domain = 'whitehouse.gov' OR
  domain = 'who.int' OR
  domain = 'wickr.com' OR
  domain = 'wikidata.org' OR
  domain = 'wikihow.com' OR
  domain = 'wikileaks.org' OR
  domain = 'wikimapia.org' OR
  domain = 'wikimedia.org' OR
  domain = 'wikipedia.org' OR
  domain = 'wiktionary.org' OR
  domain = 'wildberries.ru' OR
  domain = 'wired.com' OR
  domain = 'wisc.edu' OR
  domain = 'wix.com' OR
  domain = 'wkkf.org' OR
  domain = 'wordpress.com' OR
  domain = 'wordpress.org' OR
  domain = 'world.internationalism.org' OR
  domain = 'worldbank.org' OR
  domain = 'worldofwarcraft.com' OR
  domain = 'worldsingles.com' OR
  domain = 'wpengine.com' OR
  domain = 'write.as' OR
  domain = 'wupj.org' OR
  domain = 'www.2shared.com' OR
  domain = 'www.3wishes.com' OR
  domain = 'www.4shared.com' OR
  domain = 'www.888casino.com' OR
  domain = 'www.abcunderwear.com' OR
  domain = 'www.absolut.com' OR
  domain = 'www.accessnow.org' OR
  domain = 'www.adventist.org' OR
  domain = 'www.advocate.com' OR
  domain = 'www.agentprovocateur.com' OR
  domain = 'www.ahram.org.eg' OR
  domain = 'www.akdn.org' OR
  domain = 'www.alarabiya.net' OR
  domain = 'www.aljazeera.com' OR
  domain = 'www.aljazeera.net' OR
  domain = 'www.almanar.com.lb' OR
  domain = 'www.alqassam.ps' OR
  domain = 'www.amazon.com' OR
  domain = 'www.amnesty.org' OR
  domain = 'www.amnestyusa.org' OR
  domain = 'www.anglicancommunion.org' OR
  domain = 'www.apple.com' OR
  domain = 'www.apt.ch' OR
  domain = 'www.arabnews.com' OR
  domain = 'www.ariannelingerie.com' OR
  domain = 'www.armfor.uscourts.gov' OR
  domain = 'www.article19.org' OR
  domain = 'www.ask.com' OR
  domain = 'www.asterisk.org' OR
  domain = 'www.atheists.org' OR
  domain = 'www.avert.org' OR
  domain = 'www.bacardi.com' OR
  domain = 'www.bahai.org' OR
  domain = 'www.baidu.com' OR
  domain = 'www.barnesandnoble.com' OR
  domain = 'www.bbc.co.uk' OR
  domain = 'www.bbc.com' OR
  domain = 'www.beerinfo.com' OR
  domain = 'www.benedelman.org' OR
  domain = 'www.betternet.co' OR
  domain = 'www.bglad.com' OR
  domain = 'www.binance.com' OR
  domain = 'www.bing-amp.com' OR
  domain = 'www.bing.com' OR
  domain = 'www.birthcontrol.com' OR
  domain = 'www.bitfinex.com' OR
  domain = 'www.bittorrent.com' OR
  domain = 'www.blackberry.com' OR
  domain = 'www.blackjackinfo.com' OR
  domain = 'www.blizzard.com' OR
  domain = 'www.blockchain.com' OR
  domain = 'www.blogger.com' OR
  domain = 'www.blueskyswimwear.com' OR
  domain = 'www.bme.com' OR
  domain = 'www.broadvoice.com' OR
  domain = 'www.brookings.edu' OR
  domain = 'www.btselem.org' OR
  domain = 'www.budweiser.com' OR
  domain = 'www.business-humanrights.org' OR
  domain = 'www.buydutchseeds.com' OR
  domain = 'www.caixinglobal.com' OR
  domain = 'www.cartercenter.org' OR
  domain = 'www.casino.com' OR
  domain = 'www.casinotropez.com' OR
  domain = 'www.catholic.org' OR
  domain = 'www.cbc.ca' OR
  domain = 'www.cbsnews.com' OR
  domain = 'www.cdc.gov' OR
  domain = 'www.census.gov' OR
  domain = 'www.centcom.mil' OR
  domain = 'www.cesr.org' OR
  domain = 'www.cfr.org' OR
  domain = 'www.change.org' OR
  domain = 'www.chess.com' OR
  domain = 'www.chinadaily.com.cn' OR
  domain = 'www.chinatimes.com' OR
  domain = 'www.cia.gov' OR
  domain = 'www.circumcision.org' OR
  domain = 'www.clubhouse.com' OR
  domain = 'www.clubhouseapi.com' OR
  domain = 'www.cnn.com' OR
  domain = 'www.coinbase.com' OR
  domain = 'www.concern.net' OR
  domain = 'www.copticchurch.net' OR
  domain = 'www.coquette.com' OR
  domain = 'www.counter-strike.net' OR
  domain = 'www.cpj.org' OR
  domain = 'www.crisisgroup.org' OR
  domain = 'www.csmonitor.com' OR
  domain = 'www.dailymail.co.uk' OR
  domain = 'www.dailymotion.com' OR
  domain = 'www.dailysabah.com' OR
  domain = 'www.darpa.mil' OR
  domain = 'www.date.com' OR
  domain = 'www.datpiff.com' OR
  domain = 'www.dcma.mil' OR
  domain = 'www.dd-rd.ca' OR
  domain = 'www.dea.gov' OR
  domain = 'www.defense.gov' OR
  domain = 'www.democracynow.org' OR
  domain = 'www.dextroverse.org' OR
  domain = 'www.dharmanet.org' OR
  domain = 'www.dictionary.com' OR
  domain = 'www.doctissimo.fr' OR
  domain = 'www.dogpile.com' OR
  domain = 'www.dol.gov' OR
  domain = 'www.dotster.com' OR
  domain = 'www.douyin.com' OR
  domain = 'www.download.com' OR
  domain = 'www.dreamhost.com' OR
  domain = 'www.dropbox.com' OR
  domain = 'www.drudgereport.com' OR
  domain = 'www.dss.mil' OR
  domain = 'www.dtra.mil' OR
  domain = 'www.dw.com' OR
  domain = 'www.ea.com' OR
  domain = 'www.earthaction.org' OR
  domain = 'www.ebay.com' OR
  domain = 'www.eccouncil.org' OR
  domain = 'www.ecdc.europa.eu' OR
  domain = 'www.ecequality.org' OR
  domain = 'www.echr.coe.int' OR
  domain = 'www.economist.com' OR
  domain = 'www.edf.org' OR
  domain = 'www.eea.europa.eu' OR
  domain = 'www.eff.org' OR
  domain = 'www.eluniversal.com' OR
  domain = 'www.epa.gov' OR
  domain = 'www.episcopalchurch.org' OR
  domain = 'www.eucom.mil' OR
  domain = 'www.eurogrand.com' OR
  domain = 'www.europacasino.com' OR
  domain = 'www.eventbrite.com' OR
  domain = 'www.eveonline.com' OR
  domain = 'www.excite.com' OR
  domain = 'www.exgay.com' OR
  domain = 'www.exmormon.org' OR
  domain = 'www.f-list.net' OR
  domain = 'www.f-secure.com' OR
  domain = 'www.facebook.com' OR
  domain = 'www.familycareintl.org' OR
  domain = 'www.fark.com' OR
  domain = 'www.feminist.com' OR
  domain = 'www.feminist.org' OR
  domain = 'www.figleaves.com' OR
  domain = 'www.flickr.com' OR
  domain = 'www.foe.co.uk' OR
  domain = 'www.fondationdefrance.org' OR
  domain = 'www.fotki.com' OR
  domain = 'www.fotolog.com' OR
  domain = 'www.foxnews.com' OR
  domain = 'www.france24.com' OR
  domain = 'www.francemediasmonde.com' OR
  domain = 'www.freedominfo.org' OR
  domain = 'www.freepik.com' OR
  domain = 'www.freespeech.com' OR
  domain = 'www.freespeechcoalition.com' OR
  domain = 'www.freewebspace.com' OR
  domain = 'www.freshpair.com' OR
  domain = 'www.frtib.gov' OR
  domain = 'www.ft.com' OR
  domain = 'www.ftchinese.com' OR
  domain = 'www.gambling.com' OR
  domain = 'www.gamespot.com' OR
  domain = 'www.gatesfoundation.org' OR
  domain = 'www.gawker.com' OR
  domain = 'www.gay.com' OR
  domain = 'www.gayscape.com' OR
  domain = 'www.gearthblog.com' OR
  domain = 'www.genderhealth.org' OR
  domain = 'www.genymotion.com' OR
  domain = 'www.getdrupe.com' OR
  domain = 'www.getoutline.org' OR
  domain = 'www.giganews.com' OR
  domain = 'www.globalfundforwomen.org' OR
  domain = 'www.globalpride2020.org' OR
  domain = 'www.globalr2p.org' OR
  domain = 'www.glsen.org' OR
  domain = 'www.gmail.com' OR
  domain = 'www.gmx.com' OR
  domain = 'www.gnu.org' OR
  domain = 'www.godaddy.com' OR
  domain = 'www.gofundme.com' OR
  domain = 'www.goldenrivieracasino.com' OR
  domain = 'www.goodreads.com' OR
  domain = 'www.google.com' OR
  domain = 'www.googleapis.com' OR
  domain = 'www.gotgayporn.com' OR
  domain = 'www.gov.uk' OR
  domain = 'www.granma.cu' OR
  domain = 'www.greenpeace.org' OR
  domain = 'www.grindr.com' OR
  domain = 'www.groupon.com' OR
  domain = 'www.gstatic.com' OR
  domain = 'www.guerrillagirls.com' OR
  domain = 'www.guildwars.com' OR
  domain = 'www.gutenberg.org' OR
  domain = 'www.haaretz.com' OR
  domain = 'www.habbo.com' OR
  domain = 'www.hackforums.net' OR
  domain = 'www.hacktivismo.com' OR
  domain = 'www.hanes.com' OR
  domain = 'www.harkatulmujahideen.org' OR
  domain = 'www.health.com' OR
  domain = 'www.heritage.org' OR
  domain = 'www.hidemyass.com' OR
  domain = 'www.hightail.com' OR
  domain = 'www.hitler.org' OR
  domain = 'www.hon.ch' OR
  domain = 'www.honduras.com' OR
  domain = 'www.hootsuite.com' OR
  domain = 'www.hotspotshield.com' OR
  domain = 'www.hrc.org' OR
  domain = 'www.hrw.org' OR
  domain = 'www.humanrightsfirst.org' OR
  domain = 'www.hushmail.com' OR
  domain = 'www.icc-cpi.int' OR
  domain = 'www.icij.org' OR
  domain = 'www.icj.org' OR
  domain = 'www.iconnecthere.com' OR
  domain = 'www.ifc.org' OR
  domain = 'www.ifex.org' OR
  domain = 'www.ifj.org' OR
  domain = 'www.ifrc.org' OR
  domain = 'www.ijm.org' OR
  domain = 'www.imdb.com' OR
  domain = 'www.implantinfo.com' OR
  domain = 'www.imqq.com' OR
  domain = 'www.inbox.com' OR
  domain = 'www.indiatimes.com' OR
  domain = 'www.inetprivacy.com' OR
  domain = 'www.infomigrants.net' OR
  domain = 'www.instagram.com' OR
  domain = 'www.internationalrivers.org' OR
  domain = 'www.ipair.com' OR
  domain = 'www.ipvanish.com' OR
  domain = 'www.irna.ir' OR
  domain = 'www.iskcon.com' OR
  domain = 'www.islamicity.com' OR
  domain = 'www.isscr.org' OR
  domain = 'www.itunes.com' OR
  domain = 'www.iucn.org' OR
  domain = 'www.jackdaniels.com' OR
  domain = 'www.jackpotcitycasino1.com' OR
  domain = 'www.jdate.com' OR
  domain = 'www.jewwatch.com' OR
  domain = 'www.jhr.ca' OR
  domain = 'www.jobcorps.gov' OR
  domain = 'www.joinclubhouse.com' OR
  domain = 'www.judaism.com' OR
  domain = 'www.kabobfest.com' OR
  domain = 'www.kali.org' OR
  domain = 'www.kbs-frb.be' OR
  domain = 'www.kff.org' OR
  domain = 'www.khilafah.com' OR
  domain = 'www.knapsackforhope.org' OR
  domain = 'www.kraken.com' OR
  domain = 'www.lanacion.com.ar' OR
  domain = 'www.last.fm' OR
  domain = 'www.latimes.com' OR
  domain = 'www.lds.org' OR
  domain = 'www.leaseweb.com' OR
  domain = 'www.lemonde.fr' OR
  domain = 'www.lgbtqnation.com' OR
  domain = 'www.lightsailvpn.com' OR
  domain = 'www.lingo.com' OR
  domain = 'www.linkedin.com' OR
  domain = 'www.livejournal.com' OR
  domain = 'www.logmein.com' OR
  domain = 'www.lushstories.com' OR
  domain = 'www.macfound.org' OR
  domain = 'www.madamasr.com' OR
  domain = 'www.magicjack.com' OR
  domain = 'www.mail.com' OR
  domain = 'www.mailinator.com' OR
  domain = 'www.mainichi.co.jp' OR
  domain = 'www.malwares.com' OR
  domain = 'www.mariestopes.org.uk' OR
  domain = 'www.martus.org' OR
  domain = 'www.match.com' OR
  domain = 'www.mc-doualiya.com' OR
  domain = 'www.mediafire.com' OR
  domain = 'www.medicinenet.com' OR
  domain = 'www.meetup.com' OR
  domain = 'www.messenger.com' OR
  domain = 'www.metacrawler.com' OR
  domain = 'www.metal-archives.com' OR
  domain = 'www.microsoft.com' OR
  domain = 'www.microsofttranslator.com' OR
  domain = 'www.mizzima.com' OR
  domain = 'www.mnr.gov.cn' OR
  domain = 'www.mormon.org' OR
  domain = 'www.mozilla.org' OR
  domain = 'www.mp3.com' OR
  domain = 'www.msf.org' OR
  domain = 'www.msn.com' OR
  domain = 'www.musixmatch.com' OR
  domain = 'www.namecheap.com' OR
  domain = 'www.naral.org' OR
  domain = 'www.nato.int' OR
  domain = 'www.nature.org' OR
  domain = 'www.nbc.com' OR
  domain = 'www.nbcnews.com' OR
  domain = 'www.nbcnewyork.com' OR
  domain = 'www.nclrights.org' OR
  domain = 'www.ndi.org' OR
  domain = 'www.netflix.com' OR
  domain = 'www.newnownext.com' OR
  domain = 'www.nga.mil' OR
  domain = 'www.nifty.org' OR
  domain = 'www.northcom.mil' OR
  domain = 'www.nostraightnews.com' OR
  domain = 'www.nsa.gov' OR
  domain = 'www.nytimes.com' OR
  domain = 'www.ohchr.org' OR
  domain = 'www.onlinewomeninpolitics.org' OR
  domain = 'www.oovoo.com' OR
  domain = 'www.opendns.com' OR
  domain = 'www.openpgp.org' OR
  domain = 'www.opensocietyfoundations.org' OR
  domain = 'www.opentech.fund' OR
  domain = 'www.origin.com' OR
  domain = 'www.orthodoxconvert.info' OR
  domain = 'www.osha.gov' OR
  domain = 'www.ou.org' OR
  domain = 'www.out.com' OR
  domain = 'www.overdrive.com' OR
  domain = 'www.oxfam.org' OR
  domain = 'www.pandora.com' OR
  domain = 'www.partypoker.net' OR
  domain = 'www.patreon.com' OR
  domain = 'www.paypal.com' OR
  domain = 'www.pcgamer.com' OR
  domain = 'www.pcusa.org' OR
  domain = 'www.photobucket.com' OR
  domain = 'www.pinterest.com' OR
  domain = 'www.plannedparenthood.org' OR
  domain = 'www.playboy.com' OR
  domain = 'www.pof.com' OR
  domain = 'www.pogo.com' OR
  domain = 'www.pokerstars.com' OR
  domain = 'www.pravda.ru' OR
  domain = 'www.premaritalsex.info' OR
  domain = 'www.pridemedia.com' OR
  domain = 'www.privacyinternational.org' OR
  domain = 'www.privateinternetaccess.com' OR
  domain = 'www.projectbaseline.com' OR
  domain = 'www.protectioninternational.org' OR
  domain = 'www.pubg.com' OR
  domain = 'www.purevpn.com' OR
  domain = 'www.queerty.com' OR
  domain = 'www.quora.com' OR
  domain = 'www.rackspace.com' OR
  domain = 'www.rambler.ru' OR
  domain = 'www.ran.org' OR
  domain = 'www.rbf.org' OR
  domain = 'www.reddit.com' OR
  domain = 'www.redditstatic.com' OR
  domain = 'www.refugeesinternational.org' OR
  domain = 'www.researchgate.net' OR
  domain = 'www.reuters.com' OR
  domain = 'www.rfa.org' OR
  domain = 'www.rferl.org' OR
  domain = 'www.rfi.fr' OR
  domain = 'www.rki.de' OR
  domain = 'www.rockstargames.com' OR
  domain = 'www.rollingstone.com' OR
  domain = 'www.rollitup.org' OR
  domain = 'www.royalvegas.com' OR
  domain = 'www.rt.com' OR
  domain = 'www.runescape.com' OR
  domain = 'www.satp.org' OR
  domain = 'www.savethechildren.net' OR
  domain = 'www.sbs.com.au' OR
  domain = 'www.schwarzreport.org' OR
  domain = 'www.scientology.org' OR
  domain = 'www.scribd.com' OR
  domain = 'www.search.com' OR
  domain = 'www.securityfocus.com' OR
  domain = 'www.sendspace.com' OR
  domain = 'www.serials.ws' OR
  domain = 'www.sex.com' OR
  domain = 'www.shazam.com' OR
  domain = 'www.sida.se' OR
  domain = 'www.sierraclub.org' OR
  domain = 'www.sina.com.cn' OR
  domain = 'www.skype.com' OR
  domain = 'www.slideshare.net' OR
  domain = 'www.slotland.com' OR
  domain = 'www.snapchat.com' OR
  domain = 'www.socom.mil' OR
  domain = 'www.southcom.mil' OR
  domain = 'www.spark.com' OR
  domain = 'www.speeddater.co.uk' OR
  domain = 'www.spiegel.de' OR
  domain = 'www.spinpalace.com' OR
  domain = 'www.sportsinteraction.com' OR
  domain = 'www.spotify.com' OR
  domain = 'www.square-enix.com' OR
  domain = 'www.sss.gov' OR
  domain = 'www.starlink.com' OR
  domain = 'www.state.gov' OR
  domain = 'www.stonewall.org.uk' OR
  domain = 'www.survive.org.uk' OR
  domain = 'www.systranbox.com' OR
  domain = 'www.talkyou.me' OR
  domain = 'www.tawk.to' OR
  domain = 'www.ted.com' OR
  domain = 'www.telegraph.co.uk' OR
  domain = 'www.terredeshommes.nl' OR
  domain = 'www.theatlantic.com' OR
  domain = 'www.thegailygrind.com' OR
  domain = 'www.thegeekdiary.com' OR
  domain = 'www.theguardian.com' OR
  domain = 'www.theregister.co.uk' OR
  domain = 'www.tiktok.com' OR
  domain = 'www.tmz.com' OR
  domain = 'www.tobacco.org' OR
  domain = 'www.topdrawers.com' OR
  domain = 'www.translate.ru' OR
  domain = 'www.truecaller.com' OR
  domain = 'www.tsroadmap.com' OR
  domain = 'www.tumblr.com' OR
  domain = 'www.twitch.tv' OR
  domain = 'www.uber.com' OR
  domain = 'www.un.org' OR
  domain = 'www.unaids.org' OR
  domain = 'www.unep.org' OR
  domain = 'www.unicef.org' OR
  domain = 'www.unwatch.org' OR
  domain = 'www.upci.org' OR
  domain = 'www.uproxy.org' OR
  domain = 'www.urduvoa.com' OR
  domain = 'www.usaid.gov' OR
  domain = 'www.usatoday.com' OR
  domain = 'www.ushareit.com' OR
  domain = 'www.ushmm.org' OR
  domain = 'www.utorrent.com' OR
  domain = 'www.veoh.com' OR
  domain = 'www.vibe.com' OR
  domain = 'www.viber.com' OR
  domain = 'www.vice.com' OR
  domain = 'www.victoriassecret.com' OR
  domain = 'www.voanews.com' OR
  domain = 'www.walmart.com' OR
  domain = 'www.washingtonpost.com' OR
  domain = 'www.washingtontimes.com' OR
  domain = 'www.webs.com' OR
  domain = 'www.weebly.com' OR
  domain = 'www.weforum.org' OR
  domain = 'www.well.com' OR
  domain = 'www.wetplace.com' OR
  domain = 'www.wfp.org' OR
  domain = 'www.wftucentral.org' OR
  domain = 'www.whatsapp.com' OR
  domain = 'www.whitepages.com' OR
  domain = 'www.who.int' OR
  domain = 'www.wikidata.org' OR
  domain = 'www.wikipedia.org' OR
  domain = 'www.wiktionary.org' OR
  domain = 'www.winespectator.com' OR
  domain = 'www.witness.org' OR
  domain = 'www.wix.com' OR
  domain = 'www.wmtransfer.com' OR
  domain = 'www.wnd.com' OR
  domain = 'www.womensmediacenter.com' OR
  domain = 'www.womenwarpeace.org' OR
  domain = 'www.wordreference.com' OR
  domain = 'www.worldbank.org' OR
  domain = 'www.worldcoalition.org' OR
  domain = 'www.worldometers.info' OR
  domain = 'www.worldpulse.com' OR
  domain = 'www.worldrtd.net' OR
  domain = 'www.worldwildlife.org' OR
  domain = 'www.wsj.com' OR
  domain = 'www.xbox.com' OR
  domain = 'www.xvideos.com' OR
  domain = 'www.xxlmag.com' OR
  domain = 'www.yahoo.com' OR
  domain = 'www.yelp.com' OR
  domain = 'www.yola.com' OR
  domain = 'www.youtube.com' OR
  domain = 'www.yubo.live' OR
  domain = 'www.zeit.de' OR
  domain = 'www.zoho.com' OR
  domain = 'xanga.com' OR
  domain = 'xda-developers.com' OR
  domain = 'xe.com' OR
  domain = 'xhamster.com' OR
  domain = 'xiaomi.com' OR
  domain = 'xvideos.com' OR
  domain = 'xxx.lanl.gov' OR
  domain = 'y2mate.com' OR
  domain = 'yahoo.co.jp' OR
  domain = 'yahoo.com' OR
  domain = 'yale.edu' OR
  domain = 'yandex.com' OR
  domain = 'yandex.net' OR
  domain = 'yandex.ru' OR
  domain = 'yelp.com' OR
  domain = 'yimg.com' OR
  domain = 'youku.com' OR
  domain = 'youtu.be' OR
  domain = 'youtube.com' OR
  domain = 'ys7.com' OR
  domain = 'yt3.ggpht.com' OR
  domain = 'yt4.ggpht.com' OR
  domain = 'zdnet.com' OR
  domain = 'zemanta.com' OR
  domain = 'zh.wikipedia.org' OR
  domain = 'zillow.com' OR
  domain = 'zoho.com' OR
  domain = 'zoom.us' OR
  domain = 'zxx.edu.cn'
);

CREATE TEMP FUNCTION BadResolver(
  resolver_non_zero_rcode_rate FLOAT64,
  resolver_private_ip_rate FLOAT64,
  resolver_zero_ip_rate FLOAT64,
  resolver_connect_error_rate FLOAT64,
  resolver_invalid_cert_rate FLOAT64
) AS (
  (resolver_non_zero_rcode_rate +
   resolver_private_ip_rate +
   resolver_zero_ip_rate +
   resolver_connect_error_rate +
   resolver_invalid_cert_rate) > 0.15
);


# BASE_DATASET and DERIVED_DATASET are reserved dataset placeholder names
# which will be replaced when running the query

# Increment the version of this table if you change the table in a backwards-incomatible way.

# Rely on the table name firehook-censoredplanet.derived.merged_reduced_scans_vN
# if you would like to see a clear breakage when there's a backwards-incompatible change.
# Old table versions will be deleted.
CREATE OR REPLACE TABLE `firehook-censoredplanet.DERIVED_DATASET.reduced_satellite_scans_only_vetted_resolver_ip_info`
PARTITION BY date
# Column `country_name` is always used for filtering and must come first.
# `network`, `subnetwork`, and `domain` are useful for filtering and grouping.
CLUSTER BY country_name, network, subnetwork, domain
OPTIONS (
  friendly_name="Reduced Satellite Scans",
  description="Filtered and pre-aggregated table of Satellite scans to use with the Censored Planed Dashboard"
)
AS (
WITH Grouped AS (
    SELECT
        date,

        # As per https://docs.censoredplanet.org/dns.html#id2, some resolvers are named `special` instead of the real hostname.
        IF(resolver_name="special","special",NET.REG_DOMAIN(resolver_name)) as reg_hostname,
        resolver_name as hostname,
        resolver_as_full_name AS network,
        CONCAT("AS", resolver_asn, IF(resolver_organization IS NOT NULL, CONCAT(" - ", resolver_organization), "")) AS subnetwork,
        resolver_country AS country_code,

        IF(domain_is_control, "CONTROL", domain) AS domain,
        IFNULL(domain_category, "Uncategorized") AS category,

        OutcomeString(domain, received_error, received_rcode, answers) as outcome,
        
        COUNT(1) AS count
    FROM `firehook-censoredplanet.BASE_DATASET.satellite_scan`
    # Filter on controls_failed to potentially reduce the number of output rows (less dimensions to group by).
    WHERE domain_controls_failed = FALSE
          AND GoodDomain(domain)
          AND NOT BadResolver(resolver_non_zero_rcode_rate,
                              resolver_private_ip_rate,
                              resolver_zero_ip_rate,
                              resolver_connect_error_rate,
                              resolver_invalid_cert_rate)
          AND NOT ExtraControls(domain)
    GROUP BY date, hostname, country_code, network, subnetwork, outcome, domain, category
    # Filter it here so that we don't need to load the outcome to apply the report filtering on every filter.
    HAVING NOT STARTS_WITH(outcome, "setup/")
)
SELECT
    Grouped.* EXCEPT (country_code),
    IFNULL(country_name, country_code) AS country_name,
    CASE
        WHEN STARTS_WITH(outcome, "✅") THEN 0
        WHEN outcome = "⚠️read/udp.timeout" THEN NULL # timeouts are common in dns
        ELSE count
    END AS unexpected_count,
    CASE
        WHEN STARTS_WITH(outcome, "✅") THEN count
        WHEN outcome = "⚠️read/udp.timeout" THEN NULL
        ELSE 0
    END AS expected_count,
    FROM Grouped
    LEFT JOIN `firehook-censoredplanet.metadata.country_names` USING (country_code)
    WHERE country_code IS NOT NULL
);


# Drop the temp function before creating the view
# Since any temp functions in scope block view creation.
DROP FUNCTION ClassifySatelliteRCode;
DROP FUNCTION ClassifySatelliteError;
DROP FUNCTION OutcomeString;
DROP FUNCTION InvalidIpType;
DROP FUNCTION AnswersSignature;