package info.yangguo.yfs;

import com.google.common.collect.Lists;
import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.profile.Profile;

import java.util.Collection;
import java.util.List;

public class Cluster1Test {
    public static void main(String[] args) {
        List<Node> members = Lists.newArrayList();
        members.add(Member.builder().withId("gateway1").withAddress("127.0.0.1", 6001).build());
        members.add(Member.builder().withId("gateway2").withAddress("127.0.0.1", 6002).build());
        members.add(Member.builder().withId("gateway3").withAddress("127.0.0.1", 6003).build());

        Member member = Member.builder().withId("store").withAddress("localhost", 7001).build();
        Atomix atomix = Atomix.builder()
                .withMemberId(member.id())
                .withAddress(member.address())
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes((Collection) members)
                        .build())
                .withProfiles(Profile.client())
                .build();

        atomix.start().join();


        AtomicMap<String, String> a = atomix.getAtomicMap("store-info");

        a.put("a", "test");
        try {
            Thread.sleep(1000 * 60 * 60 * 24);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
