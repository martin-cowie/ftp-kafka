use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use unftp_core::auth::{AuthenticationError, Authenticator, Credentials, Principal};

#[derive(Debug, Deserialize)]
struct UserEntry {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct AuthConfig {
    users: Vec<UserEntry>,
}

#[derive(Debug)]
pub struct TomlAuthenticator {
    users: HashMap<String, String>,
}

impl TomlAuthenticator {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: AuthConfig = toml::from_str(&content)?;
        let users = config
            .users
            .into_iter()
            .map(|u| (u.username, u.password))
            .collect();
        Ok(TomlAuthenticator { users })
    }
}

#[async_trait]
impl Authenticator for TomlAuthenticator {
    async fn authenticate(&self, username: &str, creds: &Credentials) -> Result<Principal, AuthenticationError> {
        match &creds.password {
            Some(password) => match self.users.get(username) {
                Some(stored) if stored == password => Ok(Principal {
                    username: username.to_string(),
                }),
                _ => Err(AuthenticationError::BadPassword),
            },
            None => Err(AuthenticationError::BadPassword),
        }
    }
}
