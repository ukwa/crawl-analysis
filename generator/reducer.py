#!/usr/local/bin/python2.7

import sys
import json

sec_level_domains = ["ac", "co", "gov", "judiciary", "ltd", "me", "mod", "net", "nhs", "nic", "org", "parliament", "plc", "sch"]

current_host = None
current_host_data = {
    "ip": {},
    "mime": {},
    "virus": {},
}

for line in sys.stdin:
    host, data = line.strip().split("\t")
    data = json.loads(data)

    if current_host is None or current_host == host:
        if data["ip"] in current_host_data["ip"].keys():
            current_host_data["ip"][data["ip"]] += 1
        else:
            current_host_data["ip"][data["ip"]] = 1
        if data["mime"] in current_host_data["mime"].keys():
            current_host_data["mime"][data["mime"]] += 1
        else:
            current_host_data["mime"][data["mime"]] = 1
        if "virus" in data.keys():
            if data["virus"] in current_host_data["virus"].keys():
                current_host_data["virus"][data["virus"]] += 1
            else:
                current_host_data["virus"][data["virus"]] = 1
        current_host = host
    else:
        current_host_data["host"] = current_host
        current_host_data["tld"] = current_host.split(".")[-1]
        auth = current_host.split(".")
        if len(auth) > 2:
            sld = current_host.split(".")[-2]
            if sld in sec_level_domains:
                current_host_data["2ld"] = sld
        print json.dumps(current_host_data)
        current_host = host
        current_host_data = {
            "ip": {data["ip"]: 1},
            "mime": {data["mime"]: 1},
            "virus": {},
        }
        if "virus" in data.keys():
            if data["virus"] in current_host_data["virus"].keys():
                current_host_data["virus"][data["virus"]] += 1
            else:
                current_host_data["virus"][data["virus"]] = 1

if "host" in current_host_data.keys():
    print json.dumps(current_host_data, indent=4)

