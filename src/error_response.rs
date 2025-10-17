use actix_web::http::StatusCode;
use serde_json::json;
use chrono::Utc;
/// Helper to generate uniform error JSON responses.
pub fn error_json(error_type: &str, message: &str, code: StatusCode) -> String {
    let timestamp = Utc::now().to_rfc3339();
    json!({
        "error": {
            "type": error_type,
            "message": message,
            "code": code.as_u16(),
            "details": {},
            "timestamp": timestamp
        }
    }).to_string()
}

/// Trait for error types to provide error type string and code.
pub trait ApiError {
    fn error_type(&self) -> &'static str;
    fn error_code(&self) -> StatusCode;
}

#[macro_export]
macro_rules! impl_api_response_error {
    ($err_type:ty) => {
        impl actix_web::ResponseError for $err_type {
            fn status_code(&self) -> actix_web::http::StatusCode {
                use crate::error_response::ApiError;
                <Self as ApiError>::error_code(self)
            }
            fn error_response(&self) -> actix_web::HttpResponse {
                use crate::error_response::ApiError;
                actix_web::HttpResponse::build(self.status_code())
                    .content_type("application/json")
                    .body(crate::error_response::error_json(
                        <Self as ApiError>::error_type(self),
                        &self.to_string(),
                        self.status_code(),
                    ))
            }
        }
    };
}