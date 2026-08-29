use krabka_connect::SecretString;
use krabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct OptionSecretWithoutAttrConfig {
    password: Option<SecretString>,
}

fn main() {}
