use oxidb::OxiDb;

use super::helpers::{xml_escape, extract_xml_values};
use super::http::{HttpRequest, HttpResponse};

pub fn handle_batch_delete(db: &OxiDb, bucket: &str, req: &HttpRequest) -> HttpResponse {
    let body = String::from_utf8_lossy(&req.body);
    let keys = extract_xml_values(&body, "Key");

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for key in &keys {
        match db.delete_object(bucket, key) {
            Ok(_) | Err(_) => {
                xml.push_str(&format!("  <Deleted><Key>{}</Key></Deleted>\n", xml_escape(key)));
            }
        }
    }

    xml.push_str("</DeleteResult>");
    HttpResponse::ok_xml(xml)
}
