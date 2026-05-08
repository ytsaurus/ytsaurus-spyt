package org.apache.livy.rsc;

import tech.ytsaurus.spyt.patch.annotations.OriginClass;

import tech.ytsaurus.spyt.Utils;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.IOException;
import java.util.Properties;

@Subclass
@OriginClass("org.apache.livy.rsc.RSCConf")
public class RSCConfSpyt extends RSCConf {
    public RSCConfSpyt(Properties props) {
        super(props);
    }

    public String findLocalAddress() throws IOException {
        if (System.getenv().containsKey("YT_NETWORK_PROJECT_ID")) {
            String address = Utils.addBracketsIfIpV6Host(System.getenv().get("YT_IP_ADDRESS_DEFAULT"));
            LOG.info("MTN usage recognized. Address: {}", address);
            return address;
        } else {
            return super.findLocalAddress();
        }
    }
}
