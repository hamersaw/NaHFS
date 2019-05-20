use hdfs_comm::rpc::Protocol;
use prost::Message;
use shared::protos::{BlockIndexProto, BlockMetadataProto, GeohashIndexProto, IndexReportResponseProto, IndexReportRequestProto};

pub struct NahfsProtocol {
}

impl NahfsProtocol {
    pub fn new() -> NahfsProtocol {
        NahfsProtocol {
        }
    }

    fn index_report(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = IndexReportRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = IndexReportResponseProto::default();

        // TODO - process index report
        debug!("indexReport({:?})", request);

        response.encode_length_delimited(resp_buf).unwrap();
    }
}

impl Protocol for NahfsProtocol {
    fn process(&self, user: &Option<String>, method: &str,
            req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        match method {
            "indexReport" => self.index_report(req_buf, resp_buf),
            _ => error!("unimplemented method '{}'", method),
        }
    }
}
