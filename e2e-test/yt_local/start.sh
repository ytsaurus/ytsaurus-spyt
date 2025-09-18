#!/usr/bin/env bash

proxy_port="${PROXY_PORT:-8000}"
(socat TCP-LISTEN:"$proxy_port",fork TCP:localhost:80 &) && echo "Activated redirect from :$proxy_port to :80"

node_config="""
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
    job_proxy = {
      job_proxy_authentication_manager = {
        require_authentication=%true;
        cypress_token_authenticator={
          secure=%true;
        };
      };
    };
    signature_components={
      generation={
        generator={};
        cypress_key_writer={};
        key_rotator={}
      };
      validation={cypress_key_reader={}}
    };
  };
  job_resource_manager = {
    resource_limits = {
      cpu = 2;
      memory = 4294967296;
    };
  };
}
"""

rpc_proxy_config="""
{
  enable_shuffle_service=%true;
  signature_components={
    generation={
      generator={};
      cypress_key_writer={owner_id="test"};
      key_rotator={}
    };
    validation={cypress_key_reader={}}
  };
}
"""

source /usr/bin/start.sh --node-config "$(echo $node_config | tr -d '[:space:]')" \
  --rpc-proxy-config "$(echo $rpc_proxy_config | tr -d '[:space:]')" $@
