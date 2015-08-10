#!/usr/local/bin/python2.7

import re
import sys
import json
from urlparse import urlparse

for line in sys.stdin:
    log_time, status, size, url, discovery_path, referrer, mime, thread, request_time, hash, ignore, annotations = line.strip().split(None, 11)
    if status.isdigit() and 200 <= int(status) < 400:
        parsed_url = urlparse(url)
        host = re.sub("^(www([0-9]+)?)\.", "", parsed_url[1])
        data = {
            "mime": "".join([i if ord(i) < 128 else "" for i in mime]),
        }

        for anno in annotations.split(","):
            if ":" not in anno:
                continue
            key, value = anno.split(":", 1)
            if key == "ip":
                data["ip"] = value
            if key == "1":
                data["virus"] = value.split()[-2]

        print "%s\t%s" % (host, json.dumps(data))

