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
package com.alipay.remoting.codec;

import java.io.Serializable;

import com.alipay.remoting.Connection;
import com.alipay.remoting.Protocol;
import com.alipay.remoting.ProtocolCode;
import com.alipay.remoting.ProtocolManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.Attribute;

/**
 * 编码器，使用解码时解析到的协议，对发送的数据进行编码
 * 此编码器是无状态的，可以被每个channel使用，因此可以被标注Sharable
 *
 *
 * Protocol code based newEncoder, the main newEncoder for a certain protocol, which is lead by one or multi bytes (magic code).
 *
 * Notice: this is stateless can be noted as {@link io.netty.channel.ChannelHandler.Sharable}
 * @author jiangping
 * @version $Id: ProtocolCodeBasedEncoder.java, v 0.1 2015-12-11 PM 7:30:30 tao Exp $
 */
@ChannelHandler.Sharable
public class ProtocolCodeBasedEncoder extends MessageToByteEncoder<Serializable> {

    /** default protocol code */
    protected ProtocolCode defaultProtocolCode;

    public ProtocolCodeBasedEncoder(ProtocolCode defaultProtocolCode) {
        super();
        this.defaultProtocolCode = defaultProtocolCode;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out)
                                                                                   throws Exception {
        //获取解码时，此channel使用的协议
        Attribute<ProtocolCode> att = ctx.channel().attr(Connection.PROTOCOL);
        ProtocolCode protocolCode;
        if (att == null || att.get() == null) { //默认的协议码
            protocolCode = this.defaultProtocolCode;
        } else {
            protocolCode = att.get();
        }
         //获取协议
        Protocol protocol = ProtocolManager.getProtocol(protocolCode);
        //获取协议的编码器，进行编码
        protocol.getEncoder().encode(ctx, msg, out);
    }

}
