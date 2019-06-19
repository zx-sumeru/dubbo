package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zx
 * @since 2019/6/19 9:17
 */
public class ConsistentHashLoadBalance2 extends AbstractLoadBalance {

    public static final String NAME = "consistenthash2";

    /**
     * Hash nodes name
     */
    public static final String HASH_NODES = "hash.nodes";

    /**
     * Hash arguments name
     */
    public static final String HASH_ARGUMENTS = "hash.arguments";

    private Map<String, ConsistentHashSelector> selectorMap = new ConcurrentHashMap<>();


    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String methodName = RpcUtils.getMethodName(invocation);
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        int identityHashCode = System.identityHashCode(invokers);
        ConsistentHashSelector selector = selectorMap.get(key);
        if (selector == null || selector.identityHashCode != identityHashCode) {
            selectorMap.put(key, new ConsistentHashSelector(invokers, methodName, identityHashCode));
            selector = selectorMap.get(key);
        }
        Invoker invoker = selector.select(invocation);
        return invoker;
    }


    static class ConsistentHashSelector<T> {

        private final int replicaNumber;
        private final int identityHashCode;
        private final int[] argIndexes;

        private final TreeMap<Long, Invoker> circle;
        private final HashFunction hashFunction;

        public ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            URL url = invokers.get(0).getUrl();
            replicaNumber = url.getParameter(HASH_NODES, 160);
            this.identityHashCode = identityHashCode;
            String[] args = url.getParameter(HASH_ARGUMENTS, "0").split(",");
            argIndexes = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                argIndexes[i] = Integer.parseInt(args[i]);
            }
            this.hashFunction = new Md5HashFunction();
            circle = new TreeMap<>();
            for (Invoker invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber; i++) {
                    long hash = hashFunction.hash(address + i);
                    circle.put(hash, invoker);
                }
            }
        }

        public Invoker<T> select(Invocation invocation) {
            Object[] arguments = invocation.getArguments();
            String key = null;
            if (argIndexes.length == 1) {
                if (argIndexes[0] < 0 || argIndexes[0] > arguments.length - 1) {
                    throw new RuntimeException("argIndexes 越界");
                }
                key = String.valueOf(arguments[argIndexes[0]]);
            } else {
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < argIndexes.length; i++) {
                    if (argIndexes[i] < 0 || argIndexes[i] > arguments.length - 1) {
                        throw new RuntimeException("argIndexes 越界");
                    }
                    stringBuilder.append(arguments[argIndexes[i]]);
                }
                key = stringBuilder.toString();
            }
            long hash = hashFunction.hash(key);
            Map.Entry<Long, Invoker> entry = circle.ceilingEntry(hash);
            if (entry == null) {
                entry = circle.firstEntry();
            }
            return entry.getValue();
        }

        static interface HashFunction {
            long hash(String key);
        }

        static class Md5HashFunction implements HashFunction {

            @Override
            public long hash(String key) {
                try{
                    MessageDigest md5 = MessageDigest.getInstance("MD5");
                    md5.reset();
                    byte[] bytes = md5.digest(key.getBytes(StandardCharsets.UTF_8));
                    long hash = hash(bytes, 1);
                    return hash;
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                return 0L;
            }

            private long hash(byte[] digest, int number) {
                return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                        | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                        | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                        | (digest[number * 4] & 0xFF))
                        & 0xFFFFFFFFL;
            }
        }
    }
}

