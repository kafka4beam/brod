%% -*- mode:erlang -*-
{application,brod,
 [{description,"Kafka client"},
  {vsn,"1"},
  {registered,[]},
  {applications,[kernel,stdlib]},
  {mod,{brod_app,[]}},
  {env,[]},
  {modules,[brod,brod_consumer,brod_producer,brod_sock,brod_utils,
            kafka]}]}.
