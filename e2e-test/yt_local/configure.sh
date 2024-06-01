#!/usr/bin/env bash

cat >/etc/yt/server.yson <<EOF
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
EOF
