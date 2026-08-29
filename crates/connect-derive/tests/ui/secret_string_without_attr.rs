use krabka_connect::SecretString;
use krabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct SecretStringWithoutAttrConfig {
    password: SecretString,
}

fn main() {}
