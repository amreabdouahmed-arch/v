use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Serialize)]
pub(crate) struct ErrorResponse {
    detail: String,
}

pub(crate) struct ApiError {
    status: StatusCode,
    pub(crate) message: String,
    cause: Option<anyhow::Error>,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            cause: None,
        }
    }

    pub(crate) fn internal(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Search backend failed".to_owned(),
            cause: Some(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Some(error) = self.cause {
            eprintln!("Rust API request failed: {error:#}");
        }
        (
            self.status,
            Json(ErrorResponse {
                detail: self.message,
            }),
        )
            .into_response()
    }
}
