/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.remoting;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.log.BoltLoggerFactory;
import com.alipay.remoting.util.RemotingUtil;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

/**
 * 1、同步
 * 2、异步带有回调函数
 * 3、异步，返回future
 * 4、Oneway 不关心server的响应
 *
 * Base remoting capability.
 * 
 * @author jiangping
 * @version $Id: BaseRemoting.java, v 0.1 Mar 4, 2016 12:09:56 AM tao Exp $
 */
public abstract class BaseRemoting {
    /** logger */
    private static final Logger logger = BoltLoggerFactory.getLogger("CommonDefault");

    protected CommandFactory    commandFactory; // 生产不同的Request、Response

    public BaseRemoting(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    /**
     * 同步
     * Synchronous invocation
     * 
     * @param conn
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException 
     * @throws RemotingException
     */
    protected RemotingCommand invokeSync(final Connection conn, final RemotingCommand request,
                                         final int timeoutMillis) throws RemotingException,
                                                                 InterruptedException {
        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (!f.isSuccess()) {
                        conn.removeInvokeFuture(requestId);
                        future.putResponse(commandFactory.createSendFailedResponse(
                            conn.getRemoteAddress(), f.cause()));
                        logger.error("Invoke send failed, id={}", requestId, f.cause());
                    }
                }

            });
        } catch (Exception e) {
            conn.removeInvokeFuture(requestId);
            future.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            logger.error("Exception caught when sending invocation, id={}", requestId, e);
        }
        //阻塞timeoutMillis，如果没有响应，返回TIMEOUT Response
        RemotingCommand response = future.waitResponse(timeoutMillis);
        if (response == null) {
            conn.removeInvokeFuture(requestId);
            response = this.commandFactory.createTimeoutResponse(conn.getRemoteAddress());
            logger.warn("Wait response, request id={} timeout!", requestId);
        }

        return response;
    }

    /**
     *
     * 异步执行带有回调函数
     * Invocation with callback.
     * 
     * @param conn
     * @param request
     * @param invokeCallback
     * @param timeoutMillis
     * @throws InterruptedException
     */
    protected void invokeWithCallback(final Connection conn, final RemotingCommand request,
                                      final InvokeCallback invokeCallback, final int timeoutMillis) {
        //创建DefaultInvokeFuture，通过CountDownLatch阻塞响应
        final InvokeFuture future = createInvokeFuture(conn, request, request.getInvokeContext(),
            invokeCallback);
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                            .getRemoteAddress()));
                        future.tryAsyncExecuteInvokeCallbackAbnormally();
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout); //绑定Timeout

            //加急写，立即发送请求，不需要缓存到ChannelOutboundBuffer，但是会降低吞吐量
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) { //发送失败
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout(); //删除Timeout
                            f.putResponse(commandFactory.createSendFailedResponse(
                                conn.getRemoteAddress(), cf.cause())); //创建发送失败的响应
                            f.tryAsyncExecuteInvokeCallbackAbnormally(); //异步执行异常的回调函数
                        }
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
                f.tryAsyncExecuteInvokeCallbackAbnormally();
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
    }

    /**
     *
     * 异步执行，返回future，与同步的区别就是没有调用waitResponse(long timeoutMillis)
     * Invocation with future returned.
     * 
     * @param conn
     * @param request
     * @param timeoutMillis
     * @return
     */
    protected InvokeFuture invokeWithFuture(final Connection conn, final RemotingCommand request,
                                            final int timeoutMillis) {

        final InvokeFuture future = createInvokeFuture(request, request.getInvokeContext());
        conn.addInvokeFuture(future);
        final int requestId = request.getId();
        try {
            Timeout timeout = TimerHolder.getTimer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    InvokeFuture future = conn.removeInvokeFuture(requestId);
                    if (future != null) {
                        future.putResponse(commandFactory.createTimeoutResponse(conn
                            .getRemoteAddress()));
                    }
                }

            }, timeoutMillis, TimeUnit.MILLISECONDS);
            future.addTimeout(timeout);

            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {
                    if (!cf.isSuccess()) {
                        InvokeFuture f = conn.removeInvokeFuture(requestId);
                        if (f != null) {
                            f.cancelTimeout();
                            f.putResponse(commandFactory.createSendFailedResponse(
                                conn.getRemoteAddress(), cf.cause()));
                        }
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), cf.cause());
                    }
                }

            });
        } catch (Exception e) {
            InvokeFuture f = conn.removeInvokeFuture(requestId);
            if (f != null) {
                f.cancelTimeout();
                f.putResponse(commandFactory.createSendFailedResponse(conn.getRemoteAddress(), e));
            }
            logger.error("Exception caught when sending invocation. The address is {}",
                RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
        }
        return future;
    }

    /**
     * Oneway invocation.
     * 
     * @param conn
     * @param request
     * @throws InterruptedException
     */
    protected void oneway(final Connection conn, final RemotingCommand request) {
        try {
            conn.getChannel().writeAndFlush(request).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (!f.isSuccess()) {
                        logger.error("Invoke send failed. The address is {}",
                            RemotingUtil.parseRemoteAddress(conn.getChannel()), f.cause());
                    }
                }

            });
        } catch (Exception e) {
            if (null == conn) {
                logger.error("Conn is null");
            } else {
                logger.error("Exception caught when sending invocation. The address is {}",
                    RemotingUtil.parseRemoteAddress(conn.getChannel()), e);
            }
        }
    }

    /**
     * 创建不带有callBack的future
     * Create invoke future with {@link InvokeContext}.
     * @param request
     * @param invokeContext
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final RemotingCommand request,
                                                       final InvokeContext invokeContext);

    /**
     * 创建带有回调函数的future
     * Create invoke future with {@link InvokeContext}.
     * @param conn
     * @param request
     * @param invokeContext
     * @param invokeCallback
     * @return
     */
    protected abstract InvokeFuture createInvokeFuture(final Connection conn,
                                                       final RemotingCommand request,
                                                       final InvokeContext invokeContext,
                                                       final InvokeCallback invokeCallback);

    protected CommandFactory getCommandFactory() {
        return commandFactory;
    }
}
