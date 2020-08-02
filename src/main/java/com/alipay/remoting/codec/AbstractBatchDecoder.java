/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alipay.remoting.codec;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.StringUtil;


/**
 * 批量解包、批量提交
 *
 * This class mainly hack the {@link io.netty.handler.codec.ByteToMessageDecoder} to provide batch submission capability.
 * This can be used the same way as ByteToMessageDecoder except the case your following inbound handler may get a decoded msg,
 * which actually is an array list, then you can submit the list of msgs to an executor to process. For example
 * <pre>
 *   if (msg instanceof List) {
 *       processorManager.getDefaultExecutor().execute(new Runnable() {
 *           public void run() {
 *               // batch submit to an executor
 *               for (Object m : (List<?>) msg) {
 *                   RpcCommandHandler.this.process(ctx, m);
 *               }
 *           }
 *       });
 *   } else {
 *       process(ctx, msg);
 *   }
 * </pre>
 * You can check the method {@link AbstractBatchDecoder#channelRead(ChannelHandlerContext, Object)} ()}
 *   to know the detail modification.
 */
public abstract class AbstractBatchDecoder extends ChannelInboundHandlerAdapter {
    /**
     * Cumulate {@link ByteBuf}s by merge them into one {@link ByteBuf}'s, using memory copies.
     */
    public static final Cumulator MERGE_CUMULATOR     = new Cumulator() {
                                                          @Override
                                                          public ByteBuf cumulate(ByteBufAllocator alloc,
                                                                                  ByteBuf cumulation,
                                                                                  ByteBuf in) {
                                                              ByteBuf buffer;
                                                              if (cumulation.writerIndex() > cumulation
                                                                  .maxCapacity()
                                                                                             - in.readableBytes()
                                                                  || cumulation.refCnt() > 1) { //cumulation没有足够空间容纳新读取的数据，进行扩容
                                                                  // Expand cumulation (by replace it) when either there is not more room in the buffer
                                                                  // or if the refCnt is greater then 1 which may happen when the user use slice().retain() or
                                                                  // duplicate().retain().
                                                                  //
                                                                  // See:
                                                                  // - https://github.com/netty/netty/issues/2327
                                                                  // - https://github.com/netty/netty/issues/1764
                                                                  buffer = expandCumulation(alloc,
                                                                      cumulation,
                                                                      in.readableBytes());
                                                              } else {
                                                                  buffer = cumulation;
                                                              }
                                                              buffer.writeBytes(in); //累积新读取的数据
                                                              in.release();//及时释放内存
                                                              return buffer;
                                                          }
                                                      };

    /**
     * Cumulate {@link ByteBuf}s by add them to a {@link CompositeByteBuf} and so do no memory copy whenever possible.
     * Be aware that {@link CompositeByteBuf} use a more complex indexing implementation so depending on your use-case
     * and the decoder implementation this may be slower then just use the {@link #MERGE_CUMULATOR}.
     */
    public static final Cumulator COMPOSITE_CUMULATOR = new Cumulator() {
                                                          @Override
                                                          public ByteBuf cumulate(ByteBufAllocator alloc,
                                                                                  ByteBuf cumulation,
                                                                                  ByteBuf in) {
                                                              ByteBuf buffer;
                                                              if (cumulation.refCnt() > 1) {
                                                                  // Expand cumulation (by replace it) when the refCnt is greater then 1 which may happen when the user
                                                                  // use slice().retain() or duplicate().retain().
                                                                  //
                                                                  // See:
                                                                  // - https://github.com/netty/netty/issues/2327
                                                                  // - https://github.com/netty/netty/issues/1764
                                                                  buffer = expandCumulation(alloc,
                                                                      cumulation,
                                                                      in.readableBytes());
                                                                  buffer.writeBytes(in);
                                                                  in.release();
                                                              } else {
                                                                  CompositeByteBuf composite;
                                                                  if (cumulation instanceof CompositeByteBuf) {
                                                                      composite = (CompositeByteBuf) cumulation;
                                                                  } else {
                                                                      int readable = cumulation
                                                                          .readableBytes();
                                                                      composite = alloc
                                                                          .compositeBuffer();
                                                                      composite.addComponent(
                                                                          cumulation).writerIndex(
                                                                          readable);
                                                                  }
                                                                  composite
                                                                      .addComponent(in)
                                                                      .writerIndex(
                                                                          composite.writerIndex()
                                                                                  + in.readableBytes());
                                                                  buffer = composite;
                                                              }
                                                              return buffer;
                                                          }
                                                      };

    ByteBuf                       cumulation;
    private Cumulator             cumulator           = MERGE_CUMULATOR;
    private boolean               singleDecode;
    private boolean               decodeWasNull;
    private boolean               first;
    private int                   discardAfterReads   = 16;
    private int                   numReads;

    /**
     * If set then only one message is decoded on each {@link #channelRead(ChannelHandlerContext, Object)}
     * call. This may be useful if you need to do some protocol upgrade and want to make sure nothing is mixed up.
     *
     * Default is {@code false} as this has performance impacts.
     */
    public void setSingleDecode(boolean singleDecode) {
        this.singleDecode = singleDecode;
    }

    /**
     * If {@code true} then only one message is decoded on each
     * {@link #channelRead(ChannelHandlerContext, Object)} call.
     *
     * Default is {@code false} as this has performance impacts.
     */
    public boolean isSingleDecode() {
        return singleDecode;
    }

    /**
     * Set the {@link Cumulator} to use for cumulate the received {@link ByteBuf}s.
     */
    public void setCumulator(Cumulator cumulator) {
        if (cumulator == null) {
            throw new NullPointerException("cumulator");
        }
        this.cumulator = cumulator;
    }

    /**
     * Set the number of reads after which {@link ByteBuf#discardSomeReadBytes()} are called and so free up memory.
     * The default is {@code 16}.
     */
    public void setDiscardAfterReads(int discardAfterReads) {
        if (discardAfterReads <= 0) {
            throw new IllegalArgumentException("discardAfterReads must be > 0");
        }
        this.discardAfterReads = discardAfterReads;
    }

    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder. You usually do not need to rely on this value
     * to write a decoder. Use it only when you must use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer() internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
        return internalBuffer().readableBytes();
    }

    /**
     * Returns the internal cumulative buffer of this decoder. You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ByteBuf internalBuffer() {
        if (cumulation != null) {
            return cumulation;
        } else {
            return Unpooled.EMPTY_BUFFER;
        }
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        ByteBuf buf = internalBuffer();
        int readable = buf.readableBytes();
        if (readable > 0) {
            ByteBuf bytes = buf.readBytes(readable);
            buf.release();
            ctx.fireChannelRead(bytes);
        } else {
            buf.release();
        }
        cumulation = null;
        numReads = 0;
        ctx.fireChannelReadComplete();
        handlerRemoved0(ctx);
    }

    /**
     * Gets called after the {@link ByteToMessageDecoder} was removed from the actual context and it doesn't handle
     * events anymore.
     */
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
    }

    /**
     * This method has been modified to check the size of decoded msgs, which is represented by the
     * local variable {@code RecyclableArrayList out}. If has decoded more than one msg,
     * then construct an array list to submit all decoded msgs to the pipeline.
     *
     * @param ctx channel handler context
     * @param msg data
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            RecyclableArrayList out = RecyclableArrayList.newInstance();
            try {
                ByteBuf data = (ByteBuf) msg;
                first = cumulation == null; //判断是否第一次
                if (first) { //第一次读取数据
                    cumulation = data;
                } else { //累积已经读取的数据
                    cumulation = cumulator.cumulate(ctx.alloc(), cumulation, data);
                }
                callDecode(ctx, cumulation, out);
            } catch (DecoderException e) {
                throw e;
            } catch (Throwable t) {
                throw new DecoderException(t);
            } finally {
                if (cumulation != null && !cumulation.isReadable()) { //数据读取成功
                    numReads = 0;
                    cumulation.release(); //释放bytebuf
                    cumulation = null;
                } else if (++numReads >= discardAfterReads) {//如果连续读取16次，为了避免发生OOM，将会丢弃数据
                    // We did enough reads already try to discard some bytes so we not risk to see a OOME.
                    // See https://github.com/netty/netty/issues/4275
                    numReads = 0; //重置读取次数
                    discardSomeReadBytes(); //丢弃数据
                }

                int size = out.size();
                if (size == 0) {
                    decodeWasNull = true;
                } else if (size == 1) { //读取到一条数据
                    ctx.fireChannelRead(out.get(0));
                } else { //多条数据，封装成ArrayList
                    ArrayList<Object> ret = new ArrayList<Object>(size);
                    for (int i = 0; i < size; i++) {
                        ret.add(out.get(i));
                    }
                    ctx.fireChannelRead(ret);
                }

                out.recycle(); //对象回收，重复使用
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        numReads = 0;
        discardSomeReadBytes();
        if (decodeWasNull) {
            decodeWasNull = false;
            if (!ctx.channel().config().isAutoRead()) {
                ctx.read();
            }
        }
        ctx.fireChannelReadComplete();
    }

    protected final void discardSomeReadBytes() {
        if (cumulation != null && !first && cumulation.refCnt() == 1) {
            // discard some bytes if possible to make more room in the
            // buffer but only if the refCnt == 1  as otherwise the user may have
            // used slice().retain() or duplicate().retain().
            //
            // See:
            // - https://github.com/netty/netty/issues/2327
            // - https://github.com/netty/netty/issues/1764
            cumulation.discardSomeReadBytes();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        RecyclableArrayList out = RecyclableArrayList.newInstance();
        try {
            if (cumulation != null) {
                callDecode(ctx, cumulation, out);
                decodeLast(ctx, cumulation, out);
            } else {
                decodeLast(ctx, Unpooled.EMPTY_BUFFER, out);
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Exception e) {
            throw new DecoderException(e);
        } finally {
            try {
                if (cumulation != null) {
                    cumulation.release();
                    cumulation = null;
                }
                int size = out.size();
                for (int i = 0; i < size; i++) {
                    ctx.fireChannelRead(out.get(i));
                }
                if (size > 0) {
                    // Something was read, call fireChannelReadComplete()
                    ctx.fireChannelReadComplete();
                }
                ctx.fireChannelInactive();
            } finally {
                // recycle in all cases
                out.recycle();
            }
        }
    }

    /**
     * Called once data should be decoded from the given {@link ByteBuf}. This method will call
     * {@link #decode(ChannelHandlerContext, ByteBuf, List)} as long as decoding should take place.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added
     */
    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        try {
            while (in.isReadable()) {
                int outSize = out.size();
                int oldInputLength = in.readableBytes();
                decode(ctx, in, out);

                // Check if this handler was removed before continuing the loop.
                // If it was removed, it is not safe to continue to operate on the buffer.
                //
                // See https://github.com/netty/netty/issues/1664
                if (ctx.isRemoved()) { //判断此Handler是否已经移除，防止出现未知的错误
                    break;
                }

                if (outSize == out.size()) { //接收的数据不完整
                    if (oldInputLength == in.readableBytes()) {
                        break;
                    } else {
                        continue;
                    }
                }

                if (oldInputLength == in.readableBytes()) {//可读取的字节数没有发生变化，但是解析出了完整的数据
                    throw new DecoderException(
                        StringUtil.simpleClassName(getClass())
                                + ".decode() did not read anything but decoded a message.");
                }

                if (isSingleDecode()) {
                    break;
                }
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Throwable cause) {
            throw new DecoderException(cause);
        }
    }

    /**
     * Is called one last time when the {@link ChannelHandlerContext} goes in-active. Which means the
     * {@link #channelInactive(ChannelHandlerContext)} was triggered.
     *
     * By default this will just call {@link #decode(ChannelHandlerContext, ByteBuf, List)} but sub-classes may
     * override this for some special cleanup operation.
     */
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                                                                                      throws Exception {
        decode(ctx, in, out);
    }

    static ByteBuf expandCumulation(ByteBufAllocator alloc, ByteBuf cumulation, int readable) {
        ByteBuf oldCumulation = cumulation;
        cumulation = alloc.buffer(oldCumulation.readableBytes() + readable); //扩容之后的buffer大小=old可读取的字节+新读取的字节
        cumulation.writeBytes(oldCumulation);
        oldCumulation.release(); //释放内存
        return cumulation;
    }

    /**
     * Cumulate {@link ByteBuf}s.
     */
    public interface Cumulator {
        /**
         * Cumulate the given {@link ByteBuf}s and return the {@link ByteBuf} that holds the cumulated bytes.
         * The implementation is responsible to correctly handle the life-cycle of the given {@link ByteBuf}s and so
         * call {@link ByteBuf#release()} if a {@link ByteBuf} is fully consumed.
         */
        ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in);
    }

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
     * {@link ByteBuf} has nothing to read when return from this method or till nothing was read from the input
     * {@link ByteBuf}.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added
     * @throws Exception    is thrown if an error accour
     */
    protected abstract void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                                                                                           throws Exception;
}