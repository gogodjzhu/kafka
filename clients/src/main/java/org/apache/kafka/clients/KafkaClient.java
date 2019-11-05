/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * The interface for {@link NetworkClient}
 * 直接跟集群通信的客户端抽象
 * 作为客户端, 它定义了一下几种方法(功能):
 * 1. 连接状态的维护
 *    {@link this#ready(Node, long)} - 建立连接
 *    {@link this#disconnect(String)} - 断开连接
 *    {@link this#close(String)} - 关闭连接
 *    {@link this#isReady(Node, long)},
 *    {@link this#connectionFailed(Node)}
 *    {@link this#connectionDelay(Node, long)} - 连接状态的获取
 * 2. 消息的传输(注意这里了提供的send/poll方法均是面向ClientRequest/ClientResponse的，它封装了所有类型的请求/响应，本类只处理io，
 *      类型被忽略)
 *    {@link this#send(ClientRequest, long)} - 发送消息
 *    {@link this#poll(long, long)} - 获取socket响应
 *    {@link this#newClientRequest(String, AbstractRequest.Builder, long, boolean)} - 构造无回调的客户端请求
 *    {@link this#newClientRequest(String, AbstractRequest.Builder, long, boolean, RequestCompletionHandler)} - 构造有回
 *      调的客户端请求
 * 3. 集群状态(由于接口处理的是客户端对整个集群的通信，所以也支持面对集群的一些操作)
 *    {@link this#leastLoadedNode(long)} 集群中负载最低的节点
 *    {@link this#hasReadyNodes()} 判断是否有已建立连接的节点
 *
 *
 */
public interface KafkaClient extends Closeable {

    /**
     * Check if we are currently ready to send another request to the given node but don't attempt to connect if we
     * aren't.
     * 
     * @param node The node to check
     * @param now The current timestamp
     */
    boolean isReady(Node node, long now);

    /**
     * Initiate a connection to the given node (if necessary), and return true if already connected. The readiness of a
     * node will change only when poll is invoked.
     * 
     * @param node The node to connect to.
     * @param now The current time
     * @return true iff we are ready to immediately initiate the sending of another request to the given node.
     */
    boolean ready(Node node, long now);

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * 
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    long connectionDelay(Node node, long now);

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    boolean connectionFailed(Node node);

    /**
     * Queue up the given request for sending. Requests can only be sent on ready connections.
     * @param request The request
     * @param now The current timestamp
     */
    void send(ClientRequest request, long now);

    /**
     * Do actual reads and writes from sockets.
     * 
     * @param timeout The maximum amount of time to wait for responses in ms, must be non-negative. The implementation
     *                is free to use a lower value if appropriate (common reasons for this are a lower request or
     *                metadata update timeout)
     * @param now The current time in ms
     * @throws IllegalStateException If a request is sent to an unready node
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * Diconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    void disconnect(String nodeId);

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    void close(String nodeId);

    /**
     * Choose the node with the fewest outstanding requests. This method will prefer a node with an existing connection,
     * but will potentially choose a node for which we don't yet have a connection if all existing connections are in
     * use.
     * 
     * @param now The current time in ms
     * @return The node with the fewest in-flight requests.
     */
    Node leastLoadedNode(long now);

    /**
     * The number of currently in-flight requests for which we have not yet returned a response
     */
    int inFlightRequestCount();

    /**
     * Return true if there is at least one in-flight request and false otherwise.
     */
    boolean hasInFlightRequests();

    /**
     * Get the total in-flight requests for a particular node
     * 
     * @param nodeId The id of the node
     */
    int inFlightRequestCount(String nodeId);

    /**
     * Return true if there is at least one in-flight request for a particular node and false otherwise.
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * Return true if there is at least one node with connection in ready state and false otherwise.
     */
    boolean hasReadyNodes();

    /**
     * Wake up the client if it is currently blocked waiting for I/O
     */
    void wakeup();

    /**
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     * @param callback the callback to invoke when we get a response
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                   boolean expectResponse, RequestCompletionHandler callback);

}
