use krabka_connect::SecretString;
use krabka_connect_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct SecretDefaultConfig {
    #[config(secret, default = "password")]
    password: SecretString,
}

fn main() {}
