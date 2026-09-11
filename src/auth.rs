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
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes `content` to a uniquely-named temp file, cleaned up on drop.
    fn temp_toml(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, content.as_bytes()).unwrap();
        file
    }

    fn creds(password: Option<&str>) -> Credentials {
        match password {
            Some(p) => Credentials::from(p),
            None => Credentials {
                password: None,
                ..Credentials::from("")
            },
        }
    }

    #[test]
    fn from_file_loads_configured_users() {
        let file = temp_toml(
            r#"
                [[users]]
                username = "alice"
                password = "password123"

                [[users]]
                username = "bob"
                password = "secret456"
            "#,
        );

        let auth = TomlAuthenticator::from_file(file.path().to_str().unwrap()).unwrap();
        assert_eq!(auth.users.len(), 2);
        assert_eq!(auth.users.get("alice").map(String::as_str), Some("password123"));
    }

    #[test]
    fn from_file_fails_when_file_missing() {
        let result = TomlAuthenticator::from_file("/nonexistent/path/config.toml");
        assert!(result.is_err());
    }

    #[test]
    fn from_file_fails_on_malformed_toml() {
        let file = temp_toml("not valid toml [[[");
        let result = TomlAuthenticator::from_file(file.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authenticate_succeeds_with_correct_password() {
        let file = temp_toml(
            r#"
                [[users]]
                username = "alice"
                password = "password123"
            "#,
        );
        let auth = TomlAuthenticator::from_file(file.path().to_str().unwrap()).unwrap();

        let principal = auth.authenticate("alice", &creds(Some("password123"))).await.unwrap();
        assert_eq!(principal.username, "alice");
    }

    #[tokio::test]
    async fn authenticate_fails_with_wrong_password() {
        let file = temp_toml(
            r#"
                [[users]]
                username = "alice"
                password = "password123"
            "#,
        );
        let auth = TomlAuthenticator::from_file(file.path().to_str().unwrap()).unwrap();

        let result = auth.authenticate("alice", &creds(Some("wrong"))).await;
        assert!(matches!(result, Err(AuthenticationError::BadPassword)));
    }

    #[tokio::test]
    async fn authenticate_fails_with_unknown_user() {
        let file = temp_toml(
            r#"
                [[users]]
                username = "alice"
                password = "password123"
            "#,
        );
        let auth = TomlAuthenticator::from_file(file.path().to_str().unwrap()).unwrap();

        let result = auth.authenticate("mallory", &creds(Some("password123"))).await;
        assert!(matches!(result, Err(AuthenticationError::BadPassword)));
    }

    #[tokio::test]
    async fn authenticate_fails_without_password() {
        let file = temp_toml(
            r#"
                [[users]]
                username = "alice"
                password = "password123"
            "#,
        );
        let auth = TomlAuthenticator::from_file(file.path().to_str().unwrap()).unwrap();

        let result = auth.authenticate("alice", &creds(None)).await;
        assert!(matches!(result, Err(AuthenticationError::BadPassword)));
    }
}
