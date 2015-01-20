%% -*- mode:erlang -*-
{application,brod,
 [{description,"Kafka client"},
  {vsn,"1.0"},
  {registered,[]},
  {applications,[kernel,stdlib]},
  {mod,{brod,[]}},
  {env,[]},
  {modules,[brod,brod_consumer,brod_producer,brod_sock,brod_utils,
            kafka]}]}.
