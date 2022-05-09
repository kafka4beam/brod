# Authentication



## SASL/PLAIN

### Erlang

```erlang
[{brod,
   [{clients
     , [{kafka_client 
          , [ { endpoints, [{"localhost", 9092}] }
            , { ssl, true}
            , { sasl, {plain, "GFRW5BSQHKEH0TSG", "GrL3CNTkLhsvtBr8srGn0VilMpgDb4lPD"}}
            ]
         }
       ]
     }
   ]
}]
```

### Elixir

```elixir
import Config

config :brod,
       clients: [
         kafka_client: [
           endpoints: [
             localhost: 9092
           ],
           ssl: true,
           sasl: {
             :plain,
             System.get_env("KAFKA_USERNAME"),
             System.get_env("KAFKA_PASSWORD")
           }
         ]
       ]
```

## SSL Certificate Validation
Erlang's default configuration for SSL is [verify_none](https://github.com/erlang/otp/blob/OTP-24.3.4/lib/ssl/src/ssl_internal.hrl#L120-L218) 
which means that certificates are accepted but not validated. brod passes SSL options to the [kafka_protocol](https://hex.pm/packages/kafka_protocol) library
where they are used to create the [SSL connection](https://github.com/kafka4beam/kafka_protocol/blob/4.0.3/src/kpro_connection.erl#L305).

For more info see the Erlang Ecosystem Foundation's [server certificate verification](https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/ssl.html#server-certificate-verification) recommendations.

## Erlang
```erlang
[{brod,
   [{clients
     , [{kafka_client 
          , [ { endpoints, [{"localhost", 9092}] }
            , { ssl, [ { verify, verify_peer } 
                     , { cacertfile, "/etc/ssl/certs/ca-certificates.crt" } 
                     , { depth, 3 } 
                     , { customize_hostname_check, 
                          [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} 
                     ]}
            , { sasl, {plain, "GFRW5BSQHKEH0TSG", "GrL3CNTkLhsvtBr8srGn0VilMpgDb4lPD"}}
            ]
         }
       ]
     }
   ]
}]
```

## Elixir
```elixir
import Config

config :brod,
       clients: [
         kafka_client: [
           endpoints: [
             localhost: 9092
           ],
           ssl: [
             verify: :verify_peer,
             cacertfile: "/etc/ssl/certs/ca-certificates.crt",
             depth: 3,
             customize_hostname_check: [
               match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
             ],
           ],
           sasl: {
             :plain,
             System.get_env("KAFKA_USERNAME"),
             System.get_env("KAFKA_PASSWORD")
           }
         ]
       ]
```

The examples above are using `/etc/ssl/certs/ca-certificates.crt` which is the certificate authority that comes
with [alpine](https://hub.docker.com/_/alpine) linux. You will need to provide a path to a valid certificate authority 
certificate or use [certifi](https://hex.pm/packages/certifi)