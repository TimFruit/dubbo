package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;

/**
 * ProtocolFilterWrapper 构建过滤器连
 */
@Activate(group = CONSUMER)
public class MyConsumerFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        System.out.println("          ||| MyConsumerFilter beore");
        Result result = invoker.invoke(invocation);
        System.out.println("          ||| MyConsumerFilter after");
        return result;
    }
}
