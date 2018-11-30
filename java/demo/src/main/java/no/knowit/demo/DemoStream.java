package no.knowit.demo;

import lombok.extern.slf4j.Slf4j;
import no.knowit.demo.model.MailDto;
import no.knowit.demo.model.PostDto;
import no.knowit.demo.model.ListDto;
import no.knowit.demo.model.SubscriptionDto;
import no.knowit.demo.model.UserDto;
import no.knowit.demo.model.UserListPostInternal;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
@Slf4j
public class DemoStream {
    Properties kafkaProperties = new Properties();
    private static final String USERS = "users";
    private static final String SUBSCRIPTIONS = "subscriptions";
    private static final String POSTS = "posts";

    Serde<String> stringSerde = new Serdes.StringSerde();
    Serde<UserDto> userSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(UserDto.class));
    Serde<SubscriptionDto> subSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(SubscriptionDto.class));
    Serde<PostDto> postSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(PostDto.class));

    public DemoStream() {
        kafkaProperties.put("bootstrap.servers", "localhost:9094");
        kafkaProperties.put("application.id", "DemoStream2");
        kafkaProperties.put("auto.offset.reset", "earliest");
    }

    @PostConstruct
    public void runStream() {
        Topology topology = buildStream();
        KafkaStreams streams = new KafkaStreams(topology, kafkaProperties);
        streams.setUncaughtExceptionHandler((thread, throwable) -> log.error("Error in stream: ", throwable));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

    }

    private Topology buildStream() {
        StreamsBuilder builder = new StreamsBuilder();

        // 1. Create table of users (groupby)
        KStream<String, UserDto> userStream =
                builder.stream(USERS, Consumed.with(stringSerde, userSerde));

        userStream
                .selectKey((String key, UserDto user) -> user.getUsername())
                .to("user.info", Produced.with(stringSerde, userSerde));

        KTable<String, UserDto> userTable =
                builder.table("user.info", Consumed.with(stringSerde, userSerde));


        // 2. get number of subscriptions/subscribers (count)
        Serde<Long> longSerde = new Serdes.LongSerde();

        KStream<String, SubscriptionDto> subscriptionStream =
                builder.stream(SUBSCRIPTIONS, Consumed.with(stringSerde, subSerde));

        subscriptionStream
                .selectKey((k,v) -> v.getUsername())
                .groupByKey(Serialized.with(stringSerde, subSerde))
                .count()
                .toStream()
                .to("user.subscription-count", Produced.with(stringSerde, longSerde));

        subscriptionStream
                .groupBy((k,v) -> v.getSubscribeto(), Serialized.with(stringSerde, subSerde))
                .count()
                .toStream()
                .to("user.subscribers-count", Produced.with(stringSerde, longSerde));


        // 3. get all subscribers (aggregate)
        Serde<ListDto> listSerde =
                Serdes.serdeFrom(new JsonSerializer<>(),
                        new JsonDeserializer<>(ListDto.class));

        KTable<String, ListDto> subscribersTable =
                subscriptionStream
                        .selectKey((k,v) -> v.getSubscribeto())
                        .groupByKey(Serialized.with(stringSerde, subSerde))
                        .aggregate(
                                ListDto::new,
                                (k, v, subscriptions) -> {
                                    subscriptions.getStringList().add(v.getUsername());
                                    return subscriptions;
                                }, Materialized.with(stringSerde, listSerde));

        subscribersTable.toStream()
                .peek((k,v) -> log.info("peek: {} - {}", k, v));


        // 4. send 'mail-event' to subscribers (join, flatMap)
        Serde<MailDto> mailSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(MailDto.class));
        KStream<String, PostDto> postStream = builder.stream(POSTS, Consumed.with(stringSerde, postSerde));


        postStream
                .selectKey((k,v) -> v.getUsername())
                .join(subscribersTable, UserListPostInternal::of, Joined.with(stringSerde, postSerde, listSerde))
                .flatMap((k, v) -> {
                    List<KeyValue<String, PostDto>> keyValueList = new ArrayList<>();
                    for (String userName: v.getUserList().getStringList()) {
                        keyValueList.add(new KeyValue<>(userName, v.getPost()));
                    }
                    return keyValueList;
                })
                .join(userTable, (post, user) -> MailDto.of(user.getMail(), post.getTitle()), Joined.with(stringSerde, postSerde, userSerde))
                .to("event.mails", Produced.with(stringSerde, mailSerde));


        return builder.build();
    }
}
