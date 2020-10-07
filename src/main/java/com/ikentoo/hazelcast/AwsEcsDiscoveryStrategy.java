/*
 * Copyright (c) 2019, iKentoo SA. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ikentoo.hazelcast;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.*;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;


@SuppressWarnings("raw")
public class AwsEcsDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private final AwsEcsProperties.Config config;
    private final Set<Address> previousValues = new ConcurrentSkipListSet<>(
            comparing(Address::getHost).thenComparing(Address::getPort));

    private final String taskArn;

    public AwsEcsDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        this.taskArn = getOwnTaskArn(logger);
        this.config = AwsEcsProperties.fromProps(properties);

    }

    public static String getOwnTaskArn(ILogger logger) {
        try {

            logger.fine(format("SYSTEM_ENV=%s", System.getenv()));
            URI metaDataUri = getMetaDataUri();
            if (metaDataUri==null) {
                return null;
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(metaDataUri.toURL().openStream(), UTF_8));
            String content = reader
                    .lines()
                    .onClose(close(reader))
                    .collect(Collectors.joining(" "));

            logger.fine(format("AWS_META=%s", content));
            Pattern pattern = Pattern.compile("^.*\"TaskARN\" *: *\"([^\"]+)\".*$", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(content);

            if (!matcher.matches()) {
                logger.warning("couldn't get taskARN from content: " + content);
                return null;
            }
            String arn = matcher.group(1);
            logger.fine(format("TaskARN=%s", arn));
            return matcher.group(1);

        } catch (Exception e) {
            logger.severe("couldn't get taskARN", e);
            return null;
        }
    }

    private static URI getMetaDataUri() {
        String uri = System.getenv("ECS_CONTAINER_METADATA_URI");
        if (uri != null && !uri.isEmpty()) {
            return URI.create(uri + "/task");
        }
        uri = System.getenv("ECS_CONTAINER_METADATA_FILE");
        if (uri != null && !uri.isEmpty()) {
            if (!uri.startsWith("file:")) {
                uri = "file://"+uri;
            }
            return URI.create(uri);
        }
        return null;
    }

    private static Runnable close(BufferedReader reader) {
        return () -> {
            try {
                reader.close();
            } catch (IOException e) {
            }
        };
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        getLogger().fine(format("Discovering nodes in AWS ECS %s", config));
        try {

            AmazonECSClientBuilder clientBuilder = AmazonECSClientBuilder.standard();
            config.getAwsCredentialsProvider().ifPresent(clientBuilder::withCredentials);
            config.getAwsRegion().ifPresent(clientBuilder::withRegion);
            AmazonECS client = clientBuilder.build();

            ListTasksRequest listTaskRequest = new ListTasksRequest();
            listTaskRequest.setCluster(config.getClusterName());
            listTaskRequest.setServiceName(config.getServiceName());
            listTaskRequest.setDesiredStatus(DesiredStatus.RUNNING);
            ListTasksResult taskIds = client.listTasks(listTaskRequest);

            DescribeTasksRequest describeTaskRequest = new DescribeTasksRequest();
            describeTaskRequest.setTasks(taskIds.getTaskArns());
            describeTaskRequest.setCluster(config.getClusterName());
            DescribeTasksResult tasks = client.describeTasks(describeTaskRequest);


            List<Address> addresses = tasks.getTasks()
                    .stream()
                    // remove own task
                    .filter(task -> {
                        getLogger().fine(format("local task [%s], discovered task [%s]", this.taskArn, task.getTaskArn()));
                        return !task.getTaskArn().equals(taskArn);
                    })
                    .flatMap(this::fromTask)
                    .collect(Collectors.toList());

            previousValues.clear();
            previousValues.addAll(addresses);
        } catch (Exception e) {
            if (config.isFailFast()) {
                throw e;
            }
            getLogger().severe("Couldn't discover addresses using previous values", e);
        }
        return previousValues.stream()
                .map(SimpleDiscoveryNode::new)
                .collect(Collectors.toList());
    }

    private Stream<Address> fromTask(Task task) {
        return task.getContainers()
                .stream()
                .filter(container -> container.getName().matches(config.getContainerNameFilter()))
                .flatMap(this::fromContainer);

    }

    private Stream<Address> fromContainer(Container container) {
        return container.getNetworkInterfaces()
                .stream()
                .flatMap(this::fromNetworkInterface);
    }

    private Stream<Address> fromNetworkInterface(NetworkInterface networkInterface) {
        return config.getPorts()
                .mapToObj(port -> {
                    try {
                        return new Address(networkInterface.getPrivateIpv4Address(), port);
                    } catch (UnknownHostException e) {
                        getLogger().severe(e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull);
    }
}
