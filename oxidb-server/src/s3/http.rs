//! HTTP glue for the S3 surface. The generic HTTP request/response types and
//! parsing moved to the standalone `oxidb-http` crate (ADR-0021) so the control
//! plane can share them without linking the engine; they are re-exported here so
//! `crate::s3::http::{HttpRequest, HttpResponse, parse_request_from_reader}`
//! keeps working. Only the S3-specific XML `error_response` stays here.

pub use oxidb_http::{HttpRequest, HttpResponse, parse_request_from_reader};

pub fn error_response(status: u16, code: &str, message: &str, resource: &str) -> HttpResponse {
    let status_text = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        416 => "Requested Range Not Satisfiable",
        _ => "Internal Server Error",
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n  <Resource>{}</Resource>\n  <RequestId>0</RequestId>\n</Error>",
        super::helpers::xml_escape(code),
        super::helpers::xml_escape(message),
        super::helpers::xml_escape(resource)
    );
    HttpResponse::xml(status, status_text, xml)
}
