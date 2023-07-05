#!/usr/bin/env python

"""
Author: James Penney
Email: james.penney@deviceauthority.com
Date: 8th March 2018

Comment: An example of how to connect directly to the Device Authority DAE API to create a registration control record

Copyright Device Authority Ltd 2017-2018

Description:
    * This script provides guidance on how to create a registration control record from KeyScaler via the API

    * The authentication mechanism here uses the request details combined with the Participant Secret to generate a signature/hash of the request
    * Please see the "create_signature" method for more details on how this is computed

    * Utilizing a generated signature ensures a few things:
        1. The raw Participant Secret value is never sent explicitly in the request
        2. It helps prevent a man-in-the-middle attack where an attacker intercepts the request and modifies it for his/her own gain

    * When the KeyScaler server receives the request, it will try and compute the same signature using the request body and stored participant secret value
    * If it succeeds, it will allow the request. If it fails (because it has been modified in transit) the request is rejected


How to use:
    1. You may need some additional dependencies to run this script - use your favorite python package installer to get them
    2. Fill out the configuration details below
    3. Run the script!
"""

import urllib3
import urllib.request
import calendar
import time
import hmac
import hashlib
import base64
import string
import random
import ssl
import json
from collections import OrderedDict

# BEGIN CONFIGURATION

# set up the Participant ID and Participant Secret for the DAE
# these are passed to the KeyScalerConnector class and used for authentication to the API
# you need to get these values from your KeyScaler Control Panel on the "Account Settings" page
pid = "3ed41fbd-75c9-442d-8236-29635e5f2f6f" #61a17992-8208-4f5e-b4ad-3f45a038742f
psecret = "1763a4a2-2298-4f7f-9ae9-353b9e186e89" #79b95891-c2cb-44c7-837e-693a8b152ab8

# the base-address for your KeyScaler server (DAE)
# if you're using the eval system - this will be <tenant>.deviceauthority.com
dae_host = "dae.cummins.com"

# enable/disable certificate validation for KeyScaler comms
verify_ssl = True


# END CONFIGURATION


# this logger class is just something I use to format output to the console
class Logger:
    def err(self, msg):
        print(" [ ERR] "), msg

    def info(self, msg):
        print(" [INFO] "), msg


# this is the main connector class for communicating with the DAE
# It has several re-usable methods that could be utilized for other API calls
# - gen_nonce and create_signature
class KeyScalerConnector:
    def __init__(self, logger, pid, psecret):
        self.pid = pid
        self.psecret = psecret
        self.log = logger
        return

    def gen_nonce(self, size=8, chars=string.ascii_uppercase + string.digits):
        # return 8 random chars to salt the signature
        # this nonce is added as a header to the request
        # use a secure method for generating these
        return ''.join(random.choice(chars) for _ in range(size))

    def create_signature(self, nonce, params):
        # create signature string
        # this will be recreated on the DAE side to verify the request based on the request nonce, body and psecret
        sig_str = "%s%s%s" % (nonce, params, nonce)

        # create keyed hash message authentication code of the params
        # participant secret is the value for the key
        # We use the SHA-256 algorithm
        hmac_sig = b'hmac.new(self.psecret, sig_str, hashlib.sha256).digest()'

        # base64 encode the HMAC to produce the final signature
        b64_sig = base64.b64encode(hmac_sig)

        log.info("Request signature: %s" % b64_sig)

        # return b64 encoded signature
        return b64_sig

    def connect(self, host):

        # the URL for the request
        url = ("https://%s/service/api/v2/registration/create" % host)

        log.info("API Endpoint: %s" % url)

        # we need to timestamp the request, so get a unix timestamp value for this point in time
        timestamp = calendar.timegm(time.gmtime())

        # create the request body that will be sent to the KeyScaler server
        # we need to retain a specific order for the request to be validated on the other side, use OrderedDict class

        # DDKG
        params = OrderedDict([
            ("pid", pid),
            ("registration_controls", [
                OrderedDict([
                    ("platform", "any"),
                    ("auth_id", "jainish-test"),
                    ("ttl", 86400),
                    ("valid_from", calendar.timegm(time.gmtime()) * 1000),
                    ("device_method", "ddkg"),
                    ("device_name", "jainish-test"),
                    ("group_ids", "CosmosTestGroup"),
                    ("device_identifiers", [
                        OrderedDict([
                            ("name", "cumminsSerialNumber"),
                            ("value", "94:B8:6D:94:C2:30")
                        ])
                    ])
                ])
            ])
        ])

        """
        # PKI SIG+
        params = OrderedDict([
            ("pid", pid),
            ("registration_controls", [
                OrderedDict([
                    ("platform", "linux"),
                    ("ttl", 86400),
                    ("valid_from", calendar.timegm(time.gmtime()) * 1000),
                    ("device_method", "sigplus"),
                    ("device_name", "jp1"),
                    ("device_key", "My Reg Key"),
                    ("device_identifiers", [
                        OrderedDict([
                            ("name", "device_account_id"),
                            ("value", "jp1")
                        ])
                    ])
                ])
            ])
        ]) """

        # convert dictionary above to a string
        body = json.dumps(params, separators=(',', ':'))

        log.info("Request JSON: %s" % body)

        # get a random nonce to salt the authentication hash/signature
        nonce = self.gen_nonce()

        # create the hash/signature from the nonce and parameters
        sig = self.create_signature(nonce, body)

        # since we use SSL, create an SSL context
        ctx = ssl.create_default_context()

        if not verify_ssl:
            log.info("Server certificate verification is disabled!")
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        # set up the headers and x-headers for the request
        # includes nonce, timestamp, and hash/signature
        # KeyScaler will attempt to recreate the signature based on the participant ID and Secret that it knows
        req = urllib.request(url=url, data=body, headers={
            'Content-Type': 'application/json',
            "X-DeviceAuthority-Nonce": nonce,
            "X-DeviceAuthority-Timestamp": timestamp,
            "X-DeviceAuthority-Signature": sig,
            "X-DeviceAuthority-Version": 6
        })

        # try sending the request
        try:
            go = urllib.urlopen(req, context=ctx)

            # read the response data from the HTTP server
            response = go.read()

            # return response from KeyScaler
            return response

        # throw exception if fails
        except urllib3.HTTPError as e:
            # there was an error - print out the code and returned body
            log.err("Error connecting to KeyScaler - Response Code: %d" % e.code)
            log.err(e.read())
            return False


if __name__ == "__main__":
    # set up a universal logger class to make printing msgs and err easier
    # this is just something that I do to make my life easier
    log = Logger()

    # instantiate the KeyScalerConnector class written above
    connector = KeyScalerConnector(log, pid, psecret)

    # call the self_prov_with_reg_control method written above in the KeyScalerConnector class
    response = connector.connect(dae_host)

    # log the raw result from the KeyScaler server if the result is not "false"
    if response:
        log.info("KeyScaler Response: %s" % response)
    else:
        # log return an error if we failed to connect to KeyScaler
        log.err("Encountered an error connecting to DAE")