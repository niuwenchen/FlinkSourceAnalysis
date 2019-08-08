/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jackniu.flink.runtime.io.network.nerry;

import com.jackniu.flink.runtime.io.network.NetworkSequenceViewReader;
import com.jackniu.flink.runtime.io.network.TaskEventDispatcher;
import com.jackniu.flink.runtime.io.network.partition.PartitionNotFoundException;
import com.jackniu.flink.runtime.io.network.partition.ResultPartitionProvider;
import com.jackniu.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	private final PartitionRequestQueue outboundQueue;

	private final boolean creditBasedEnabled;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventDispatcher taskEventDispatcher,
		PartitionRequestQueue outboundQueue,
		boolean creditBasedEnabled) {

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.outboundQueue = outboundQueue;
		this.creditBasedEnabled = creditBasedEnabled;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == NettyMessage.PartitionRequest.class) {
				NettyMessage.PartitionRequest request = (NettyMessage.PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					NetworkSequenceViewReader reader;
					if (creditBasedEnabled) {
						reader = new CreditBasedSequenceNumberingViewReader(
							request.receiverId,
							request.credit,
							outboundQueue);
					} else {
						reader = new SequenceNumberingViewReader(
							request.receiverId,
							outboundQueue);
					}

					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);

					outboundQueue.notifyReaderCreated(reader);
				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == NettyMessage.TaskEventRequest.class) {
				NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) msg;

				if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == NettyMessage.CancelPartitionRequest.class) {
				NettyMessage.CancelPartitionRequest request = (NettyMessage.CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == NettyMessage.CloseRequest.class) {
				outboundQueue.close();
			} else if (msgClazz == NettyMessage.AddCredit.class) {
				NettyMessage.AddCredit request = (NettyMessage.AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
