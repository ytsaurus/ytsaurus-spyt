#!/usr/bin/env bash

proxy_port="${PROXY_PORT:-8000}"
(socat TCP-LISTEN:"$proxy_port",fork TCP:localhost:80 &) && echo "Activated redirect from :$proxy_port to :80"

node_config="
{
  address_resolver = {
    enable_ipv4 = true;
    enable_ipv6 = false;
  };
  exec_node = {
    job_controller = {
      resource_limits = {
        cpu = 2;
        memory = 4294967296;
      };
    };
  };
  job_resource_manager = {
    resource_limits = {
      cpu = 2;
      memory = 4294967296;
    };
  };
}
"

source /usr/bin/start.sh --node-config "$(echo $node_config | tr -d '[:space:]')" $@
